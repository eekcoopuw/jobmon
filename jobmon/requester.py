import json
import logging
import os
import warnings
import zmq

from jobmon import exceptions


class Requester(object):
    """Sends messages to a Responder node through zmq. sends messages to a
    Responder via request dictionaries which the Responder node consumes and
    responds to. A common use case is where the swarm of application jobs send
    status messages to a Responder in the CentralJobStateMonitor

    Args
        out_dir (string): file path where the server configuration is
            stored.
        monitor_host (string): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        monitor_port (int): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        request_retries (int, optional): How many times to attempt to contact
            the central job monitor. Default=3
        request_timeout (int, optional): How long to wait for a response from
            the central job monitor. Default=3 seconds
    """

    def __init__(self, out_dir=None, monitor_host=None, monitor_port=None,
                 request_retries=3, request_timeout=3000):
        """set class defaults. attempt to connect with server."""
        self.logger = logging.getLogger(__name__)
        if not (bool(out_dir) ^ bool(monitor_host and monitor_port)):
            raise ValueError("Either out_dir or the combination monitor_host+"
                             "monitor_port must be specified. Cannot specify "
                             "both out_dir and a host+port pair.")
        self.request_retries = request_retries
        self.request_timeout = request_timeout
        self.poller = None
        self.socket = None
        self.message_id = 0

        try:
            if out_dir:
                self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
                with open("%s/monitor_info.json" % self.out_dir) as f:
                    self.mi = json.load(f)
            else:
                self.mi = {'host': monitor_host, 'port': monitor_port}
            self.connect()
        except IOError as e:
            self.logger.error("Failed to connect in Requester.__init__, "
                              "exception: {}".format(e))
            warnings.warn("Unable to connect to server")

    def connect(self):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        context = zmq.Context()  # default 1 i/o thread
        self.socket = context.socket(zmq.REQ)  # blocks socket on send
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        self.logger.info('{}: Connecting...'.format(os.getpid()))

        # use host and port from network filesystem cofig. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=self.mi['host'], sp=self.mi['port']))

        # setup zmq poller to poll for messages on the socket connection.
        # we only register 1 socket but poller supports multiple.
        self.poller = zmq.Poller()  # poll
        self.poller.register(self.socket, zmq.POLLIN)

    def is_connected(self):
        return self.poller is not None

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        self.logger.info('{}: Disconnecting...'.format(os.getpid()))
        self.socket.close()
        self.poller.unregister(self.socket)

        # Good idea to release these so that they get garbage collected.
        # They might have OS memory
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
            self.logger.debug(reply)
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
        retries_left = self.request_retries
        self.logger.debug('{}: Sending message id {}: {}'.format(
            os.getpid(), self.message_id, message))
        reply = 0
        while retries_left:
            # Reconnect if necessary
            if self.socket is None or self.socket.closed:
                self.connect()
            self.socket.send_json(message)  # send message to server
            expect_reply = True
            while expect_reply:
                # ask for response from server. wait until REQUEST_TIMEOUT
                socks = dict(self.poller.poll(self.request_timeout))
                if socks.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv_json()
                    if not reply:
                        reply = 0
                        break
                    else:
                        retries_left = 0
                        expect_reply = False
                        self.logger.debug(
                            '{}: Received reply for message id {}: {}'.format(
                                os.getpid(), self.message_id, reply))
                else:
                    self.logger.info("No response from server, retrying...")
                    self.disconnect()
                    retries_left -= 1
                    if retries_left == 0:
                        self.logger.info(
                            ("{}: Server seems to be offline, abandoning"
                             " message id {}").format(os.getpid(),
                                                      self.message_id))
                        reply = 0
                        raise exceptions.NoResponseReceived(
                            "No response recieved from responder in {} retries"
                            " after waiting for {} seconds each try.".format(
                                str(self.request_retries),
                                str(self.request_timeout)))
                    self.connect()
                    self.logger.debug(
                        '  {}: resending message...{}'.format(os.getpid(),
                                                              message))
                    self.socket.send_json(message)
        return reply
