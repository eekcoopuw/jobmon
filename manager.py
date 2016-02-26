import zmq
import os
import json

REQUEST_TIMEOUT = 3000
REQUEST_RETRIES = 3


class Manager(object):

    def __init__(self, out_dir):
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.connect()

    def connect(self):
        with open("%s/monitor_info.json" % self.out_dir) as f:
            mi = json.load(f)
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        print 'Connecting...'
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=mi['host'], sp=mi['port']))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def disconnect(self):
        print 'Disconnecting...'
        self.socket.close()
        self.poller.unregister(self.socket)

    def send_request(self, message, verbose=False):
        if isinstance(message, dict):
            message = json.dumps(message)
        reply = self.send_lazy_pirate(message)
        if verbose is True:
            print reply
        return reply

    def send_lazy_pirate(self, message):
        retries_left = REQUEST_RETRIES
        print 'Sending message...'
        while retries_left:
            if self.socket.closed:
                self.connect()
            self.socket.send(message)
            expect_reply = True
            while expect_reply:
                socks = dict(self.poller.poll(REQUEST_TIMEOUT))
                if socks.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv()
                    if not reply:
                        reply = 0
                        break
                    else:
                        retries_left = 0
                        expect_reply = False
                else:
                    print "W: No response from server, retrying..."
                    self.disconnect()
                    retries_left -= 1
                    if retries_left == 0:
                        print "E: Server seems to be offline, abandoning"
                        reply = 0
                        break
                    self.connect()
                    print 'Sending message...'
                    self.socket.send(message)
        return reply

    def stop_monitor(self):
        self.send_reqest('stop')
