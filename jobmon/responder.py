import zmq

from socket import gethostname
import os
import sys
import json
import inspect
from jobmon.exceptions import ReturnCodes
from multiprocessing import Process

from jobmon.setup_logger import setup_logger


assert sys.version_info > (3, 0), """
    Sorry, only Python version 3+ is supported at this time"""


def get_class_that_defined_method(meth):
    """Utility for getting the class names of actions made available to the
    responder

    Lifted from this SO post:
    http://stackoverflow.com/questions/3589311/get-defining-class-of-unbound-method-object-in-python-3/25959545#25959545
    """
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
    return None  # not required since None would have been implicitly returned


class MonitorAlreadyRunning(Exception):
    def __init__(self, monfile):
        super(MonitorAlreadyRunning, self).__init__(
            "A monitor already exists. To safely create a new monitor, "
            "terminate the process listed in '{}' and delete the "
            "file".format(monfile))


class Responder(object):
    """This really is a server, in that there is one of these, it listens on a
    Receiver object (a zmq channel) and does stuff as a result of those
    commands.  A singleton in the directory. Runs as a separate process, not in
    the same process that started the qmaster.

    Error Codes
    INVALID_RESPONSE_FORMAT
    INVALID_ACTION
    GENERIC_ERROR

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
        self.server_pid = None
        self.server_proc = None

        self.actions = []
        self.register_object_actions(self)

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
        monfn = '%s/monitor_info.json' % self.out_dir
        logmsg = '{}: Writing connection info to {}'.format(os.getpid(),
                                                            monfn)
        Responder.logger.debug(logmsg)
        if os.path.exists(monfn):
            raise MonitorAlreadyRunning(monfn)
        with open(monfn, 'w') as f:
            json.dump({'host': host, 'port': port, 'pid': os.getpid()}, f)

    def _open_socket(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on receive
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name, self.port)  # dump config

    def start_server(self, nonblocking=True):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server."""
        Responder.logger.info('{}: Opening socket...'.format(os.getpid()))
        Responder.logger.info('{}: Responder starting...'.format(os.getpid()))
        if nonblocking:
            self.server_proc = Process(target=self.listen)
            self.server_proc.start()
            self.server_pid = self.server_proc.pid
        else:
            self._open_socket()
            self.listen()
        return self.port, self.socket

    def stop_server(self):
        """Stops response server. Only applicable if listening in a
        non-blocking subprocess"""
        if self.server_proc is not None:
            if self.server_proc.is_alive():
                self.server_proc.terminate()
                exitcode = self.server_proc.exitcode
                try:
                    os.remove('%s/monitor_info.json' % self.out_dir)
                except Exception:
                    Responder.logger.info("monitor_info.json file already "
                                          "deleted")
        else:
            Responder.logger.info("Response server is already stopped")
            exitcode = None
        return exitcode

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
            Responder.logger.info('Socket not created. Attempting to open.')
            self._open_socket()
            Responder.logger.info('Socket opened successfully')
        Responder.logger.info('Responder started, port {}.'.format(self.port))
        keep_alive = True
        while keep_alive:
            msg = self.socket.recv_json()  # server blocks on receive
            Responder.logger.debug("Received json {}".format(msg))
            try:
                if msg == 'stop':
                    keep_alive = False
                    logmsg = "{}: Responder Stopping".format(os.getpid())
                    Responder.logger.info(logmsg)
                    self.socket.send_json((ReturnCodes.OK, "Responder stopping"))
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

                    response = tocall(*act_args, **act_kwargs)
                    if self.is_valid_response(response):
                        logmsg = '{}: Responder sending response {}'.format(
                            os.getpid(), response)
                        Responder.logger.debug(logmsg)
                        self.socket.send_json(response)
                    else:
                        Responder.logger.error("action has invalid response format: {}".format(response))
                        response = (ReturnCodes.INVALID_RESPONSE_FORMAT, "action has invalid response format")
                        self.socket.send_json(response)
            except AttributeError as e:
                logmsg = "{} is not a valid action for this Responder".format(
                    msg['action'])
                Responder.logger.exception(logmsg)
                response = (ReturnCodes.INVALID_ACTION, logmsg)
                self.socket.send_json(response)
                raise e
            except Exception as e:
                logmsg = (
                    '{}: Responder sending "generic problem" error: '
                    '{}'.format(os.getpid(), e))
                Responder.logger.debug(logmsg)
                response = (ReturnCodes.GENERIC_ERROR, "Uh oh, something went wrong")
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
