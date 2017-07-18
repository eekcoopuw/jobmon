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

    def __init__(self, out_dir=None, publisher_host=None, publisher_port=None):

        if not (bool(out_dir) ^ bool(publisher_host and publisher_port)):
            raise ValueError("Either out_dir or the combination "
                             "publisher_host+publisher_port must be "
                             "specified. Cannot specify both out_dir and "
                             "a host+port pair.")
        self.logger = logging.getLogger(__name__)
        self.socket = None
        if out_dir:
            self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
            self.out_dir = os.path.realpath(self.out_dir)
            with open("{}/publisher_info.json".format(self.out_dir)) as f:
                self.mi = json.load(f)
        else:
            self.mi = {'host': publisher_host, 'port': publisher_port}

    def connect(self, topicfilter=None, timeout=1000):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored. This will ALWAYS connect."""
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout)
        if topicfilter:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
        self.logger.info('{}: Connecting...'.format(os.getpid()))

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

    def recieve_update(self):
        try:
            topic, result = demogrify(self.socket.recv().decode("utf-8"))
            return result
        except zmq.Again as e:
            logging.error("Error receiving update {}".format(logging.error(e)))
            return None
