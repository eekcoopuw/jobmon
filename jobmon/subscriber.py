import logging
import zmq

from jobmon.pubsub_helpers import demogrify


logger = logging.getLogger(__name__)


class Subscriber(object):

    def __init__(self, host, port, topic=""):

        ctx = zmq.Context()
        self.socket = ctx.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
        self.socket.connect("tcp://{}:{}".format(host, port))

    def receive(self):
        """This is blocking by design"""
        _, msg = demogrify(self.socket.recv().decode("utf-8"))
        return msg
