import logging
import sys
import traceback
from enum import Enum
from socket import gethostname
import os
import json
import inspect
from multiprocessing import Process
from threading import Thread, Event

import zmq

from jobmon.exceptions import ReturnCodes

logger = logging.getLogger(__name__)

class ServerProcType(Enum):
    SUBPROCESS = 1
    THREAD = 2
    NONE = 3


def get_class_that_defined_method(meth):
    """Utility for getting the class names of actions made available to the
    responder

    Lifted from this SO post:
    http://stackoverflow.com/questions/3589311/get-defining-class-of-unbound-method-object-in-python-3/25959545#25959545
    """
    if sys.version_info > (3, 0):
        if inspect.ismethod(meth):
            for cls in inspect.getmro(meth.__self__.__class__):
                if cls.__dict__.get(meth.__name__) is meth:
                    return cls
            meth = meth.__func__   # fallback to __qualname__ parsing
        if inspect.isfunction(meth):
            cls = getattr(inspect.getmodule(meth),
                          meth.__qualname__.split(
                              '.<locals>', 1)[0].rsplit('.', 1)[0])
            if isinstance(cls, type):
                return cls
        return None  # not required. None would have been implicitly returned
    else:
        for cls in inspect.getmro(meth.im_class):
            if meth.__name__ in cls.__dict__:
                return cls
        return None


class MonitorAlreadyRunning(Exception):
    def __init__(self, monfile):
        super(MonitorAlreadyRunning, self).__init__(
            "A monitor already exists. To safely create a new monitor, "
            "terminate the process listed in '{}' and delete the "
            "file".format(monfile))


class Responder(object):
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
    logger = None
    _keep_alive = True

    def __init__(self, out_dir, port=None):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read

        Args:
            out_dir (str): The directory where the connection settings are
                to be stored so Requesters know which endpoint to communicate
                with
            port (int): Port that the reponder should listen on. If None
                (default), the system will choose the port
        """
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except OSError:  # It throws if the directory already exists!
            pass
        self.port = port
        self.socket = None
        self.server_pid = None
        self.server_proc_type = None
        self.server_proc = None
        self.thread_stop_request = None

        self.actions = []
        self.register_object_actions(self)

    @property
    def node_name(self):
        """name of node that server is running on"""
        return gethostname()

    @property
    def keep_alive(self):
        if self.thread_stop_request:
            if self.thread_stop_request.isSet():
                self._keep_alive = False
        return self._keep_alive

    @keep_alive.setter
    def keep_alive(self, val):
        self._keep_alive = val

    def write_connection_info(self, host, port):
        """dump server connection configuration to network filesystem for
        client nodes to read using json.dump

        Args:
            host (string): node name that server is running on
            port (int): port that server is listening at
        """
        monfn = '%s/monitor_info.json' % self.out_dir
        logmsg = '{pid}: Writing connection info {h}:{p} to {f}'.format(pid=os.getpid(), h=host, p=port, f=monfn)
        logger.debug(logmsg)
        if os.path.exists(monfn):
            raise MonitorAlreadyRunning(monfn)
        with open(monfn, 'w') as f:
            json.dump({'host': host, 'port': port, 'pid': os.getpid()}, f)
            f.write("\n")

    def _open_socket(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on receive
        if self.port is None:
            self.port = self.socket.bind_to_random_port('tcp://*')
        else:
            self.socket.bind('tcp://*:{}'.format(self.port))
        self.write_connection_info(self.node_name, self.port)  # dump config

    def start_server(self, server_proc_type=ServerProcType.SUBPROCESS):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server."""
        logger.info('{}: Opening socket...'.format(os.getpid()))
        logger.info('{}: Responder starting...'.format(os.getpid()))
        self.server_proc_type = server_proc_type

        # using multiprocessing
        if self.server_proc_type == ServerProcType.SUBPROCESS:
            self.server_proc = Process(target=self.listen)
            self.server_proc.start()
            self.server_pid = self.server_proc.pid
        # using threading for in memory sqlite db
        elif self.server_proc_type == ServerProcType.THREAD:
            self.thread_stop_request = Event()
            self.server_proc = Thread(target=self.listen)
            self.server_proc.start()
        # synchronous
        elif self.server_proc_type == ServerProcType.NONE:
            self._open_socket()
            self.listen()
        else:
            raise TypeError(
                "server_proc_type must be enumerated on ServerProcType")
        return self.port, self.socket

    def stop_server(self):
        """Stops response server. Only applicable if listening in a
        non-blocking subprocess. Threads and blocking execution exit normally
        """
        exitcode = ReturnCodes.UNKNOWN_EXIT_STATE
        if self.server_proc_type == ServerProcType.SUBPROCESS:
            if self.server_proc.is_alive():
                self.server_proc.terminate()
                exitcode = self.server_proc.exitcode
                file_name = '{}/monitor_info.json'.format(self.out_dir)
                try:
                    os.remove(file_name)
                except OSError as e:
                    logger.info("Could not delete monitor_info.json file '{}', error: {}".format(file_name, e))
        elif self.server_proc_type == ServerProcType.THREAD:
            if self.server_proc.is_alive():
                # set the threading EVENT to True
                self.thread_stop_request.set()

                # next prod the server to make it cycle with a simple requester
                context = zmq.Context()
                socket = context.socket(zmq.REQ)
                socket.connect("tcp://{}:{}".format(self.node_name, self.port))
                socket.send_json({"action": "cycle"})

                # then join the threads
                self.server_proc.join(timeout=10)
                exitcode = ReturnCodes.OK
                file_name = '{}/monitor_info.json'.format(self.out_dir)
                try:
                    os.remove(file_name)
                except OSError as e:
                    logger.info("Could not delete monitor_info.json file '{}', error: {}".format(file_name, e))
        elif self.server_proc_type == ServerProcType.NONE:
            logger.info("Response server is already stopped")
            exitcode = None
        else:
            raise TypeError(
                "server_proc_type must be enumerated on ServerProcType")
        return exitcode

    def _close_socket(self):
        """stops listening at network socket/port."""
        logger.info('Stopping server...')
        os.remove('{}/monitor_info.json'.format(self.out_dir))
        self.socket.close()
        logger.info('Responder stopped.')
        return True

    def restart_server(self):
        """restart listening at new network socket/port"""
        logger.info('Restarting server...')
        self._close_socket()
        self.start_server()

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

        # TODO: I think this could be run in the background using a
        # multiprocessing.Process, which would make it quite a bit easier to
        # interact with at the python-object level. Will require
        # some investigation.

        # NOTE: JSON was chosen as the serialization method here over
        # pickle as it does not tie the implementation to python and because
        # JSON is arguable smaller and faster:
        #
        #   http://www.benfrederickson.com/dont-pickle-your-data/
        #
        if self.socket is None:
            logger.info('Socket not created. Attempting to open.')
            self._open_socket()
            logger.info('Socket opened successfully')
        logger.info('Responder started, port {}.'.format(self.port))
        while self.keep_alive:
            msg = self.socket.recv_json()  # server blocks on receive
            logger.debug("Received json {}".format(msg))
            try:
                if msg == 'stop':
                    self.keep_alive = False
                    logmsg = "{}: Responder Stopping".format(os.getpid())
                    logger.info(logmsg)
                    self.socket.send_json(
                        (ReturnCodes.OK, "Responder stopping"))
                    self._close_socket()
                else:
                    # An actual application message, use introspection to find
                    # the handler

                    tocall = [act for act in self.actions if
                              act.__name__ == "_action_{}".format(
                                  msg['action'])][0]

                    if 'kwargs' in msg.keys():
                        act_kwargs = msg['kwargs']
                    else:
                        act_kwargs = {}

                    if 'args' in msg.keys():
                        act_args = msg['args']
                    else:
                        act_args = []
                    logger.debug("Calling action {}".format(tocall))
                    response = tocall(*act_args, **act_kwargs)
                    if self.is_valid_response(response):
                        logmsg = '{}: Responder sending response {}'.format(
                            os.getpid(), response)
                        logger.debug(logmsg)
                        self.socket.send_json(response)
                    else:
                        logger.error(
                            "action has invalid response format: {}".format(
                                response))
                        response = (ReturnCodes.INVALID_RESPONSE_FORMAT,
                                    "action has invalid response format")
                        self.socket.send_json(response)
            except AttributeError as e:
                logmsg = "{} is not a valid action for this Responder".format(
                    msg['action'])
                logger.exception(logmsg)
                response = (ReturnCodes.INVALID_ACTION, logmsg)
                self.socket.send_json(response)
                raise e
            except Exception as e:
                logmsg = (
                    '{}: Responder sending "generic problem" error: '
                    '{}'.format(os.getpid(), e))
                logger.debug(logmsg)
                traceback.print_exc()
                response = (ReturnCodes.GENERIC_ERROR, logmsg)
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
        logger.debug(logmsg)
        return (ReturnCodes.OK, "Yes, I am alive")

    def _action_cycle(self):
        """A simple dummy 'action' that forces the server while loop to cycle
        """
        logmsg = "{}: Responder received cycle?".format(os.getpid())
        logger.debug(logmsg)
        return (ReturnCodes.OK, "Forced cycle of server")

    def register_action(self, action):
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
        if not(action.__name__.startswith('_action_')):
            raise NameError("Methods to be exposed to requesters as 'actions' "
                            "must have names prefixed with _action_")
        self.actions.append(action)

    def register_object_actions(self, obj):
        """Register all of obj's methods that are prefixed with _action_ as
        invokable by a requester"""
        for act_name in dir(obj):
            if act_name.startswith('_action_'):
                self.register_action(getattr(obj, act_name))

    def inspect_actions(self):
        """Return basic information about available actions... useful info
        might be action name, class where method is defined, and the
        arguments to the method"""
        return [{'name': a.__name__,
                 'defining-class': get_class_that_defined_method(a)}
                for a in self.actions]
