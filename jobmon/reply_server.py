import logging
import os
import traceback
from socket import gethostname

import zmq

from jobmon.connection_config import ConnectionConfig
from jobmon.exceptions import InvalidAction, InvalidRequest, InvalidResponse, \
    ReturnCodes
from jobmon.requester import Requester

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


logger = logging.getLogger(__name__)


class ReplyServer(object):
    """This really is a server, in that there is one of these, it listens on a
    Receiver object (a zmq channel) and does stuff as a result of those
    commands.  A singleton in the directory. Runs as a separate process or
    thread.

    Error Codes
    INVALID_RESPONSE_FORMAT
    INVALID_ACTION
    GENERIC_ERROR

    Args:
        out_dir (string): full filepath of directory to write server config in
    """

    def __init__(self, port=None):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read

        Args:
            port (int): Port that the responder should listen on. If None
                (default), the system will choose the port
        """
        self.port = port
        self.socket = None

        self.actions = {}
        self.register_action('alive', self._is_alive)

    def close_socket(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def open_socket(self):
        context = zmq.Context.instance()
        self.socket = context.socket(zmq.REP)  # server blocks on receive
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        if self.port is None:
            self.port = self.socket.bind_to_random_port('tcp://*')
        else:
            self.socket.bind('tcp://*:{}'.format(self.port))
        logger.info("Listening on port {}".format(self.port))
        return self.node_name, self.port

    def listen(self):
        """Run server. Start consuming registration and status updates from
        jobs. Use introspection to call the handler method

        Upon receiving a message, replies with a tuple (error_code, message).
        In this implementation, return codes are defined in expections.py

        """
        # TODO: Actions should probably be their own class or interface. Think
        # about how that should be done. 'Alive' should be a good simple Action
        # to start with. For now, let's just adopt a convention that
        # action methods should be prefixed with _action_. Each action should
        # return a tuple, where the first element is an integer error code.
        # The second element can pretty much be anything, as long as it is
        # json-serializable. See self.is_valid_response.

        # NOTE: JSON was chosen as the serialization method here over
        # pickle as it does not tie the implementation to python and because
        # JSON is arguable smaller and faster:
        #
        #   http://www.benfrederickson.com/dont-pickle-your-data/
        #
        if self.socket is None:
            logger.info('Socket not created. Attempting to open.')
            self.open_socket()
        logger.info('Listening on port {}.'.format(self.port))
        while True:
            try:
                # Random chatter on the socket has been leading to decode
                # errors on non-json messages
                # (e.g. JSONDecodeError: Expecting value: line 1 column 1
                #       (char 0))
                #
                # We don't believe these are "real" errors, but rather
                # random networking/monitoring blips. Logging and
                # continuing for now.
                msg = self.socket.recv_json()  # server blocks on receive
                try:
                    if msg == 'stop':
                        logger.info("ReplyServer stopping")
                        self.socket.send_json(
                            (ReturnCodes.OK, "ReplyServer stopping"))
                        break
                    else:
                        self._validate_request(msg)
                        response = self._process_message(msg)
                        self._validate_response(response)
                        logmsg = 'Replying with: {}'.format(response)
                        logger.debug(logmsg)
                        self.socket.send_json(response)
                except InvalidResponse:
                    logger.error(
                        "action has invalid response format: {}".format(
                            response))
                    response = (ReturnCodes.INVALID_RESPONSE_FORMAT,
                                "action has invalid response format")
                    self.socket.send_json(response)
                except InvalidRequest:
                    logger.error(
                        "action has invalid request format: {}".format(
                            msg))
                    response = (ReturnCodes.INVALID_REQUEST_FORMAT,
                                "action has invalid request format")
                    self.socket.send_json(response)

                except InvalidAction:
                    logmsg = ("{} is not a valid action for this "
                              "ReplyServer. Available actions: {}".format(
                                  msg['action'], ",".join(self.actions)))
                    logger.exception(logmsg)
                    response = (ReturnCodes.INVALID_ACTION, logmsg)
                    self.socket.send_json(response)
                except Exception:
                    logmsg = (
                        'ReplyServer sending "generic" error: {}'
                        .format(traceback.format_exc()))
                    logger.debug(logmsg)
                    traceback.print_exc()
                    response = (ReturnCodes.GENERIC_ERROR, logmsg)
                    self.socket.send_json(response)
            except (JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(e)
        self.close_socket()

    def register_action(self, name, action):
        """Register a method as an action that can be invoked
        by a requester.

        Args:
            action (method): A method that can be invoked by a requester.
                The name of the method should begin with "_action_" and the
                remainder of the method name will be used for the invocation by
                default. For example, the method _action_alive can be invoked
                by a requester using the alias "alive" (Renaming or
                calling-class scoping may be implemented later to avoid
                name-conflicts across methods)
        """
        self.actions[name] = action

    def stop_listening(self):
        cc = ConnectionConfig('localhost', self.port)
        req = Requester(cc)
        req.connect()
        req.send_request('stop')
        req.disconnect()

    @property
    def node_name(self):
        """name of node that server is running on"""
        return gethostname()

    def _is_alive(self):
        """A simple 'action' that sends a response to the requester indicating
        that this responder is in fact listening"""
        logmsg = "{}: Responder received is_alive?".format(os.getpid())
        logger.debug(logmsg)
        return (ReturnCodes.OK, "Yes, I am alive")

    def _process_message(self, msg):
        # An actual application message, use introspection to find
        # the handler
        action_handle = msg['action']
        if action_handle not in self.actions:
            raise InvalidAction
        action = self.actions[action_handle]
        if 'kwargs' in msg.keys():
            act_kwargs = msg['kwargs']
        else:
            act_kwargs = {}
        if 'args' in msg.keys():
            act_args = msg['args']
        else:
            act_args = []
        response = action(*act_args, **act_kwargs)
        return response

    def _validate_request(self, request):
        """validate that action method returns value in expected format.

        Args:
            request (object): any object can be accepted by this method but
                only dicts of the form

                    {   'action': 'some_action_handle',
                        'args': [1, 'arg2'],
                        'kwargs' {'kw1': 1, 'kw2': 'mocks'}
                    }
                are considered valid requests. 'args' and 'kwargs' may be
                omitted, but 'action' is mandatory.
        """
        if not isinstance(request, dict):
            raise InvalidRequest
        if 'action' not in request:
            raise InvalidRequest
        if set(request.keys()) - {'action', 'args', 'kwargs'}:
            raise InvalidRequest
        return True

    def _validate_response(self, response):
        """validate that action method returns value in expected format.

        Args:
            response (object): any object can be accepted by this method but
                only tuples of the form (response_code, response message) are
                considered valid responses. response_code must be an integer.
                response message can by any byte string.
        """
        if not (isinstance(response, tuple) and isinstance(response[0], int)):
            raise InvalidResponse
        return True
