import logging
import json
import os

import zmq


class Subscriber(object):
    """
    Args
        out_dir (string): file path where the server configuration is
            stored.
    """

    def __init__(self, out_dir):

        self.logger = logging.getLogger(__name__)
        self.socket = None

    def connect(self, topicfilter=None):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        with open("{}/monitor_info.json".format(self.out_dir)) as f:
            mi = json.load(f)
        context = zmq.Context()  # default 1 i/o thread
        self.socket = context.socket(zmq.SUB)  # blocks socket on send

        if topicfilter:
            self.socket.setsockopt(zmq.SUBSCRIBE, topicfilter)
        self.logger.info('{}: Connecting...'.format(os.getpid()))

        # use host and port from network filesystem cofig. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=mi['host'], sp=mi['port']))

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        self.logger.info('{}: Disconnecting...'.format(os.getpid()))
        self.socket.close()

        # Good idea to release this so that it gets garbage collected.
        self.socket = None

    def recieve_update(self):
        string = self.socket.recv()
        return string
