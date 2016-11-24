import zmq
from socket import gethostname
import os
import sys
import json

from jobmon.setup_logger import setup_logger

assert sys.version_info > (3, 0), """
    Sorry, only Python version 3+ are supported at this time"""


class Responder(object):
    """This really is a server, in that there is one of these, it listens on a
    Receiver object (a zmq channel) and does stuff as a result of those
    commands.  A singleton in the directory. Runs as a separate process, not in
    the same process that started the qmaster.

    Args:
        out_dir (string): full filepath of directory to write server config in
    """
    logger = None

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read

        Args:
            out_dir (str): The directory where the connection settings are
                to be stored so Requesters know which endpoint to communicate
                with
        """
        if not Responder.logger:
            Responder.logger = setup_logger('central_monitor',
                                            'central_monitor_logging.yaml')
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except FileExistsError:  # It throws if the directory already exists!
            pass
        self.port = None
        self.socket = None

    @property
    def node_name(self):
        """name of node that server is running on"""
        return gethostname()

    def write_connection_info(self, host, port):
        """dump server connection configuration to network filesystem for
        client nodes to read using json.dump

        Args:
            host (string): node name that server is running on
            port (int): port that server is listening at
        """
        filename = '%s/monitor_info.json' % self.out_dir
        logmsg = '{}: Writing connection info to {}'.format(os.getpid(),
                                                            filename)
        Responder.logger.debug(logmsg)
        with open(filename, 'w') as f:
            json.dump({'host': host, 'port': port}, f)

    def _open_socket(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on receive
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name, self.port)  # dump config

    def start_server(self):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server."""
        Responder.logger.info('{}: Opening socket...'.format(os.getpid()))
        self._open_socket()
        Responder.logger.info('{}: Responder starting...'.format(os.getpid()))
        self.listen()
        return self.port, self.socket

    def _close_socket(self):
        """stops listening at network socket/port."""
        Responder.logger.info('Stopping server...')
        os.remove('%s/monitor_info.json' % self.out_dir)
        self.socket.close()
        Responder.logger.info('Responder stopped.')
        return True

    def restart_server(self):
        """restart listening at new network socket/port"""
        Responder.logger.info('Restarting server...')
        self._close_socket()
        self.start_server()

    def listen(self):
        """Run server. Start consuming registration and status updates from
        jobs. Use introspection to call the handler method

        Upon receiving a message, replies with a tuple (error_code, message).
        In this implementation, error codes are:
            0 = Responder stopping
            1 = Action has invalid response
            2 = Requested action has not been implemented on this Responder
            3 = Unhandled generic exception
            4+ = ... To be defined on an Action-by-Action basis
        """
        # TODO: Actions should probably be their own class or interface. Think
        # about how that should be done. 'Alive' should be a good simple Action
        # to start with. For now, let's just adopt a convention that
        # action methods should be prefixed with _action_. Each action should
        # return a tuple, where the first element is an integer error code.
        # The second element can pretty much be anything, as long as it is
        # json-serializable. See self.is_valid_response.
        if self.socket.closed:
            Responder.logger.info('Socket close. Attempting to re-open.')
            self._open_socket()
        Responder.logger.info('Responder started.')
        keep_alive = True
        while keep_alive:
            msg = self.socket.recv_json()  # server blocks on receive
            print(msg)
            try:
                if msg == 'stop':
                    keep_alive = False
                    logmsg = "{}: Responder Stopping".format(os.getpid())
                    Responder.logger.info(logmsg)
                    self.socket.send_json((0, "Responder stopping"))
                    self._close_socket()
                else:
                    # An actual application message, use introspection to find
                    # the handler
                    tocall = getattr(self, "_action_{}".format(msg['action']))
                    if 'kwargs' in msg.keys():
                        act_kwargs = msg['kwargs']
                    else:
                        act_kwargs = {}
                    if 'args' in msg.keys():
                        act_args = msg['args']
                    else:
                        act_args = []

                    response = tocall(*act_args, **act_kwargs)
                    if self.is_valid_response(response):
                        logmsg = '{}: Responder sending response {}'.format(
                            os.getpid(), response)
                        Responder.logger.debug(logmsg)
                        self.socket.send_json(response)
                    else:
                        response = (1, "action has invalid response format")
                        self.socket.send_json(response)
            except AttributeError as e:
                logmsg = "{} is not a valid action for this Responder".format(
                    msg['action'])
                Responder.logger.exception(logmsg)
                response = (2, logmsg)
                self.socket.send_json(response)
                raise e
            except Exception as e:
                logmsg = (
                    '{}: Responder sending "generic problem" error '
                    '{}'.format(os.getpid(), e))
                Responder.logger.debug(logmsg)
                response = (3, "Uh oh, something went wrong")
                self.socket.send_json(response)

    def is_valid_response(self, response):
        """validate that action method returns value in expected format.

        Args:
            response (object): any object can be accepted by this method but
                only tuples of the form (response_code, response message) are
                considered valid responses. response_code must be an integer.
                response message can by any byte string.
        """
        return isinstance(response, tuple) and isinstance(response[0], int)

    def _action_alive(self):
        """A simple 'action' that sends a response to the requester indicating
        that this responder is in fact listening"""
        logmsg = "{}: Responder received is_alive?".format(os.getpid())
        Responder.logger.debug(logmsg)
        return 0, "Yes, I am alive"
