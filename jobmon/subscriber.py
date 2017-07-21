import logging
import json
import os

import zmq


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

        self.logger = logging.getLogger(__name__)
        self.socket = None
        self.mi = publisher_connection.load_monitor_info()

    def connect(self, topicfilter=None, timeout=1000):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout)
        if topicfilter:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
        self.logger.info('{}: Connecting to {}:{}...'.format(os.getpid(), self.mi['host'], self.mi['port']))

        # use host and port from network filesystem cofig. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=self.mi['host'], sp=self.mi['port']))

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        self.logger.info('{}: Disconnecting...'.format(os.getpid()))
        self.socket.close()

        # Good idea to release this so that it gets garbage collected.
        self.socket = None

    def receive_update(self):
        try:
            topic, result = demogrify(self.socket.recv().decode("utf-8"))
            return result
        except zmq.Again as e:
            # This can occur if there is no data yet
            logging.debug("Non-fatal error receiving update {}".format(logging.error(e)))
            return None

    def recieve_update(self):
        """Deprecated form of the interface with a spelling error. This call exists so as not to break old code."""
        return self.receive_update()
