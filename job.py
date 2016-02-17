import zmq
import os
import json
from logging import Handler
import sge
from subprocess import CalledProcessError

REQUEST_TIMEOUT = 3000
REQUEST_RETRIES = 3


class ZmqHandler(Handler):

    def __init__(self, job):
        Handler.__init__()
        self.job = job

    def emit(self, record):
        self.job.log_error(record.message)


class Job(object):

    def __init__(self, out_dir, jid=None, name=None):
        if jid is None:
            self.jid = os.getenv("JOB_ID")
        else:
            self.jid = jid
        if name is None:
            self.name = os.getenv("JOB_NAME")
        else:
            self.name = name

        # Try to get job_details
        try:
            self.job_info = sge.qstat_details(self.jid)[self.jid]
            if self.name is None:
                self.name = self.job_info['job_name']
        except CalledProcessError:
            self.job_info = None

        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.connect()
        self.register()

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

    def register(self):
        if self.job_info is not None:
            msg = {
                'action': 'create_job',
                'args': [self.jid],
                'kwargs': {
                    'name': self.name,
                    'runfile': self.job_info['script_file'],
                    'args': self.job_info['job_args']}}
        else:
            msg = {
                'action': 'create_job',
                'args': [self.jid],
                'kwargs': {'name': self.name}}
        self.send_request(msg)

    def start(self):
        msg = {'action': 'update_job_status', 'args': [self.jid, 2]}
        self.send_request(msg)

    def failed(self):
        msg = {'action': 'update_job_status', 'args': [self.jid, 3]}
        self.send_request(msg)

    def log_error(self, msg):
        msg = json.dumps({
            'action': 'log_error',
            'args': [self.jid, msg]})
        self.send_request(msg)

    def finish(self):
        msg = {'action': 'update_job_status', 'args': [self.jid, 4]}
        self.send_request(msg)


def log_exceptions(job):
    def wrapper(func):
        def catch_and_send(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception, e:
                job.log_error(str(e))
                raise
        return catch_and_send
    return wrapper
