
import json
import logging
import os
import pickle
import warnings
import zmq

from jobmon.setup_logger import setup_logger

__mod_name__ = "jobmon"
logger = logging.getLogger(__mod_name__)
setup_logger(logger.name, "./jobmon.log", logging.DEBUG)


class Sender(object):
    """Sends messages to a Receiver node through
    zmq. sends messages to a Receiver via request dictionaries which the Receiver
    node consumes and responds to. A common use case is where the swarm of application
    jobs send status messages to a Receiver in the CentralJobStateMonitor

    Args
        out_dir (string): file path where the server configuration is
            stored.
    """
    def __init__(self, out_dir, request_retries=3, request_timeout=3000):
        """set class defaults. attempt to connect with server."""
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.request_retries = request_retries
        self.request_timeout = request_timeout
        self.poller = None
        self.socket = None
        self.message_id = 0
        try:
            self.connect()
        except IOError as e:
            logger.error("Failed to connect in Sender.__init__, exception: {}".format(e))
            warnings.warn("Unable to connect to server")

    def connect(self):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWASY connect."""
        logger.debug("{}: Sender attempting to read monitor_info.json".format(os.getpid()))
        with open("%s/monitor_info.json" % self.out_dir) as f:
            mi = json.load(f)
        context = zmq.Context()  # default 1 i/o thread
        self.socket = context.socket(zmq.REQ)  # blocks socket on send
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        logger.info('{}: Connecting...'.format(os.getpid()))

        # use host and port from network filesystem cofig. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=mi['host'], sp=mi['port']))

        # setup zmq poller to poll for messages on the socket connection.
        # we only register 1 socket but poller supports multiple.
        self.poller = zmq.Poller()  # poll
        self.poller.register(self.socket, zmq.POLLIN)

    def is_connected(self):
        return self.poller is not None

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        logger.info('{}: Disconnecting...'.format(os.getpid()))
        self.socket.close()
        self.poller.unregister(self.socket)
        # Good idea to release these so that they get garbage collected. They might have OS memory
        self.poller = None
        self.socket = None

    def send_request(self, message, verbose=False):
        """send request dictionary to server. Need to document what form this
        dict takes.

        Args:
            message (dict): what should this look like????????????
            verbose (bool, optional): Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message
        """
        if isinstance(message, dict):
            message = json.dumps(message)
        reply = self._send_lazy_pirate(message)
        if verbose is True:
            logger.debug(reply)
        return reply

    def _send_lazy_pirate(self, message):
        """Safely send messages to server. This is not an api method, use send_request method instead

        Args:
            message (dict): what should this look like????????????

        Returns:
            Server reply message
        """
        self.message_id += 1
        retries_left = self.request_retries
        logger.debug('{}: Sending message id {}: {}'.format(os.getpid(), self.message_id, message))
        reply = 0
        while retries_left:
            if self.socket is None or self.socket.closed:  # connect to socket if disconnected?
                self.connect()
            self.socket.send_string(message)  # send message to server
            expect_reply = True
            while expect_reply:
                # ask for response from server. wait until REQUEST_TIMEOUT
                socks = dict(self.poller.poll(self.request_timeout))
                if socks.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv()
                    if not reply:
                        reply = 0
                        break
                    else:
                        reply = pickle.loads(reply)
                        retries_left = 0
                        expect_reply = False
                        logger.debug('{}: Received reply for message id {}: {}'.format(os.getpid(), self.message_id,
                                                                                       reply))
                else:
                    logger.info("No response from server, retrying...")
                    self.disconnect()
                    retries_left -= 1
                    if retries_left == 0:
                        logger.info("{}: Server seems to be offline, abandoning message id {}".format(os.getpid(),
                                                                                                      self.message_id))
                        reply = 0
                        break
                    self.connect()
                    logger.debug('  {}: resending message...{}'.format(os.getpid(), message))
                    self.socket.send_string(message)
        return reply
