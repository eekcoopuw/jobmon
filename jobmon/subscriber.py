import logging
import json
import os
import sys

import zmq

from jobmon.setup_logger import setup_logger

logger = logging.getLogger(__name__)


def demogrify(topicmsg):
    """Inverse of publisher.mogrify()"""
    json0 = topicmsg.find('{')
    topic = topicmsg[0:json0].strip()
    msg = json.loads(topicmsg[json0:])
    return topic, msg


class Subscriber(object):
    """
    Args
        out_dir (string): file path where the server configuration is
            stored.
        publisher_host (string): in lieu of a filepath to the publisher info,
            you can specify the hostname and port directly
        publisher_port (int): in lieu of a filepath to the publisher info,
            you can specify the hostname and port directly
    """

    def __init__(self, publisher_connection=None):

        logger = logging.getLogger(__name__)
        self.socket = None
        self.mi = publisher_connection.load_monitor_info()

    def connect(self, topicfilter=None, timeout=1000):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.SUB)

        # use host and port from network filesystem config. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=self.mi['host'], sp=self.mi['port']))

        self.socket.setsockopt(zmq.RCVTIMEO, timeout)
        if topicfilter:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
        else:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, '')
        logger.info('{}: Connecting to {}:{}; filter {}...'.format(os.getpid(), self.mi['host'], self.mi['port'], topicfilter))



    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        logger.info('{}: Disconnecting...'.format(os.getpid()))
        self.socket.close()

        # Good idea to release this so that it gets garbage collected.
        self.socket = None


    def receive_update(self):
        """This is not-blocking be design, so that qstats can be done"""
        try:
            x = self.socket.recv()
            # logger.debug("   received {}".format(x))
            topic, result = demogrify(x.decode("utf-8"))
            return result
        except zmq.Again as e:
            # This will occur if there is no data yet
            # logging.debug("  No data available when receiving update {}".format(logging.error(e)))
            return None

    def recieve_update(self):
        """Deprecated form of the interface with a spelling error. This call exists so as not to break old code."""
        return self.receive_update()
