from builtins import str
import logging
import zmq

from jobmon.pubsub_helpers import demogrify


logger = logging.getLogger(__name__)


class Subscriber(object):

    def __init__(self, host, port, topic=""):

        zmq_context = zmq.Context.instance()
        self.socket = zmq_context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        self.socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
        self.socket.connect("tcp://{}:{}".format(host, port))

    def disconnect(self):
        self.socket.close()

    def receive(self):
        """This is blocking by design"""
        _, msg = demogrify(self.socket.recv().decode("utf-8"))
        return msg
