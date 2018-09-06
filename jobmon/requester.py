import logging
import os
import warnings
import zmq

from jobmon import exceptions


logger = logging.getLogger(__name__)


class Requester(object):
    """Sends messages to a Responder node through zmq. sends messages to a
    Responder via request dictionaries which the Responder node consumes and
    responds to. A common use case is where the swarm of application jobs send
    status messages to a Responder in the CentralJobStateMonitor

    Args
        connection_config (ConnectionConfig): host and port info for a remote
            jobmon instance
    """

    def __init__(self, connection_config):
        """set class defaults. attempt to connect with server."""

        self.conn_cfg = connection_config
        self.poller = None
        self.socket = None
        self.message_id = 0

        try:
            self.connect()
        except IOError as e:
            logger.error("Failed to connect in Requester.__init__, "
                         "exception: {}".format(e))
            warnings.warn("Unable to connect to server")

    def connect(self):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        zmq_context = zmq.Context.instance()
        self.socket = zmq_context.socket(zmq.REQ)  # blocks socket on send
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        logger.debug('{}: Connecting to {}:{}...'.format(os.getpid(),
                                                         self.conn_cfg.host,
                                                         self.conn_cfg.port))

        # use host and port from network filesystem config. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=self.conn_cfg.host,
                                     sp=self.conn_cfg.port))

        # setup zmq poller to poll for messages on the socket connection.
        # we only register 1 socket but poller supports multiple.
        self.poller = zmq.Poller()  # poll
        self.poller.register(self.socket, zmq.POLLIN)

    @property
    def is_connected(self):
        return self.poller is not None

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        logger.debug('{}: Disconnecting from {}:{} ...'.format(
            os.getpid(), self.conn_cfg.host, self.conn_cfg.port))
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()

        self.poller = None
        self.socket = None

    def send_request(self, message, verbose=False):
        """send request to server. Need to document what form this message
        takes.

        Args:
            message (dict): The message dict must at minimum have an 'action'
                keyword. For example, a valid message might be:

                    {'action': 'write_msg_todb',
                     'msg': 'some message to be written to the db'}

                Valid actions and further key-value pairs are to be defined
                in sub-class Requester=Responder pairs.

            verbose (bool, optional): Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message
        """
        reply = self._send_lazy_pirate(message)
        if verbose is True:
            logger.debug(reply)
        return reply

    def _send_lazy_pirate(self, message):
        """Safely send messages to server. This is not an api method, use
        send_request method instead

        Args:
            message (dict): The message dict must at minimum have an 'action'
                keyword. For example, a valid message might be:

                    {'action': 'write_msg_todb',
                     'msg': 'some message to be written to the db'}

                Valid actions and further key-value pairs are to be defined
                in sub-class Requester=Responder pairs.


        Returns: Server reply message
        """
        self.message_id += 1
        retries_left = self.conn_cfg.request_retries
        logger.debug('{}: Sending message id {}: {} to {}:{}'.format(
            os.getpid(), self.message_id, message, self.conn_cfg.host,
            self.conn_cfg.port))
        reply = 0
        while retries_left:
            if not self.socket:
                break
            self.socket.send_json(message)  # send message to server
            expect_reply = True
            while expect_reply and self.is_connected:
                # ask for response from server. wait until REQUEST_TIMEOUT
                try:
                    socks = dict(self.poller.poll(
                        self.conn_cfg.request_timeout))
                except:
                    logger.warning("Pirate.poll caught an exception, "
                                   "retrying {}"
                                   .format(retries_left))
                    retries_left -= 1
                    continue

                if socks.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv_json()
                    if not reply:
                        reply = 0
                        break
                    else:
                        retries_left = 0
                        expect_reply = False
                        logger.debug(
                            '{}: Received reply for message id {}: {}'.format(
                                os.getpid(), self.message_id, reply))
                else:
                    logger.info("No response from server, retrying...")
                    self.disconnect()
                    retries_left -= 1
                    if retries_left == 0:
                        logger.info(
                            ("{}: Server seems to be offline, abandoning"
                             " message id {}").format(os.getpid(),
                                                      self.message_id))
                        reply = 0
                        raise exceptions.NoResponseReceived(
                            "No response received from responder at {}:{} in "
                            "{} retries after waiting for {} seconds each "
                            "try.".format(
                                self.conn_cfg.host,
                                self.conn_cfg.port,
                                str(self.conn_cfg.request_retries),
                                str(self.conn_cfg.request_timeout)))
                    self.connect()
                    logger.debug(
                        '  {}: resending message...{}'.format(os.getpid(),
                                                              message))
                    self.socket.send_json(message)
        return reply
