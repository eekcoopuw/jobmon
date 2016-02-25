import zmq
import os
import json
import pickle
from logging import Handler
import sge
from subprocess import CalledProcessError

REQUEST_TIMEOUT = 3000
REQUEST_RETRIES = 3


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


class ZmqHandler(Handler):

    def __init__(self, job):
        super().__init__()
        self.job = job

    def emit(self, record):
        self.job.log_error(record.message)


class Job(object):
    """client node. connects to server node through
    zmq. sends messages to server via request dictionaries which the server
    node consumes and responds to.

    Args
        out_dir (string): file path where the server configuration is
            stored.
    """
    def __init__(self, out_dir):
        """set class defaults. attempt to connect with server."""
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.connect()

    def connect(self):
        """Connect to server. Reads config file from out_dir specified during
        class instantiation to get socket. Not an API method,
        needs to be underscored"""
        with open("%s/monitor_info.json" % self.out_dir) as f:
            mi = json.load(f)
        context = zmq.Context()  # default 1 i/o thread
        self.socket = context.socket(zmq.REQ)  # blocks socket on send
        self.socket.setsockopt(zmq.LINGER, 0)  # do not pile requests in queue.
        print 'Connecting...'

        # use host and port from network filesystem cofig. option "out_dir"
        self.socket.connect(
            "tcp://{sh}:{sp}".format(sh=mi['host'], sp=mi['port']))

        # setup zmq poller to poll for messages on the socket connection.
        # we only register 1 socket but poller supports multiple.
        self.poller = zmq.Poller()  # poll
        self.poller.register(self.socket, zmq.POLLIN)

    def disconnect(self):
        """disconnect from socket and unregister with poller. Is this an API
        method? Should be underscored if not"""
        print 'Disconnecting...'
        self.socket.close()
        self.poller.unregister(self.socket)

    def send_request(self, message, verbose=False):
        """send request dictionary to server. Need to document what form this
        dict takes.

        Args:
            message (dict): what should this look like????????????
            verbose (bool, optional): Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message
        """
        if isinstance(message, dict):
            message = json.dumps(message)
        reply = self.send_lazy_pirate(message)
        if verbose is True:
            print reply
        return reply

    def send_lazy_pirate(self, message):
        """Safely send messages to server. This is not an api method. should
        perhaps be underscored. Use send_request method instead

        Args:
            message (dict): what should this look like????????????

        Returns:
            Server reply message
        """
        retries_left = REQUEST_RETRIES  # this should be configurable
        print 'Sending message...'
        while retries_left:
            if self.socket.closed:  # connect to socket if disconnected?
                self.connect()
            self.socket.send(message)  # send message to server
            expect_reply = True
            while expect_reply:
                # ask for response from server. wait until REQUEST_TIMEOUT
                socks = dict(self.poller.poll(REQUEST_TIMEOUT))
                if socks.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv()
                    if not reply:
                        reply = 0
                        break
                    else:
                        reply = pickle.loads(reply)
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


class SGEJob(Job):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        out_dir (string): file path where the server configuration is
            stored.
        jid (int, optional): job id of current process on SGE. If job id is not
            specified, will attempt to use environment variable JOB_ID.
        name (string, optional): name current process. If name is not specified
            will attempt to use environment variable JOB_NAME.
    """
    def __init__(self, jid=None, name=None, *args, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified. args & kwargs passed through to super
        class aka "Job"
        """
        super(SGEJob, self).__init__(*args, **kwargs)

        if jid is None:
            self.jid = int(os.getenv("JOB_ID"))
        else:
            self.jid = int(jid)
        if name is None:
            self.name = os.getenv("JOB_NAME")
        else:
            self.name = name

        try:
            self.job_info = sge.qstat_details(self.jid)[self.jid]
            if self.name is None:
                self.name = self.job_info['job_name']
        except CalledProcessError:
            self.job_info = None

        self.register()

    def register(self):
        """send registration request to server. server will create database
        entry for this job."""
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
        """log job start with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 2]}
        self.send_request(msg)

    def failed(self):
        """log job failure with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 3]}
        self.send_request(msg)

    def log_error(self, msg):
        """log job error with server"""
        msg = json.dumps({
            'action': 'log_error',
            'args': [self.jid, msg]})
        self.send_request(msg)

    def finish(self):
        """log job complete with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 4]}
        self.send_request(msg)

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.send_request(msg)
        return response
