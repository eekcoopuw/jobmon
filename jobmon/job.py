import zmq
import os
import json
import pickle
import subprocess
import time
import warnings
from logging import Handler
import sge
from subprocess import CalledProcessError

REQUEST_TIMEOUT = 3000
REQUEST_RETRIES = 3

this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))


class ServerRunning(Exception):
    pass


class ServerStartLocked(Exception):
    pass


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


class Client(object):
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
        try:
            self.connect()
        except IOError:
            warnings.warn("no monitor_info.json found in specified directory."
                          " Unable to connect to server")

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


class Job(Client):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        out_dir (string): file path where the server configuration is
            stored.
        jid (int, optional): job id to use when registering with jobmon
            database. If job id is not specified, will attempt to use
            environment variable JOB_ID.
        name (string, optional): name current process. If name is not specified
            will attempt to use environment variable JOB_NAME.
    """
    def __init__(self, out_dir, jid=None, name=None):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        super(Job, self).__init__(out_dir)

        # get sge_id from envirnoment
        self.sge_id = os.getenv("JOB_ID")
        if self.sge_id is not None:
            self.sge_id = int(self.sge_id)

        if jid is None:
            self.reserve_jid()
        else:
            self.jid = jid

        if name is None:
            self.name = os.getenv("JOB_NAME")
        else:
            self.name = name

        # Try to get job_details
        try:
            self.job_info = sge.qstat_details(self.sge_id)[self.sge_id]
            if self.name is None:
                self.name = self.job_info['job_name']
        except CalledProcessError:
            self.job_info = {'script_file': 'N/A', 'args': 'N/A'}
        for reqdkey in ['script_file', 'job_args']:
            if reqdkey not in self.job_info.keys():
                self.job_info[reqdkey] = 'N/A'
        self.register_sgejob()

    def reserve_jid(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'create_job', 'args': ''}
        r = self.send_request(msg)
        self.jid = r[1]

    def register_sgejob(self):
        """log specific details related to sge job status."""
        if self.sge_id is not None:
            msg = {
                'action': 'create_sgejob',
                'args': '',
                'kwargs': {
                    'jid': self.jid,
                    'name': self.name,
                    'sgeid': self.sge_id,
                    'runfile': self.job_info['script_file'],
                    'args': self.job_info['job_args']}}
        else:
            msg = {
                'action': 'create_sgejob',
                'args': '',
                'kwargs': {
                    'jid': self.jid,
                    'name': self.name}}
        self.send_request(msg)

    def start(self):
        """log job start with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 3]}
        self.send_request(msg)

    def failed(self):
        """log job failure with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 4]}
        self.send_request(msg)

    def log_error(self, msg):
        """log job error with server"""
        msg = json.dumps({
            'action': 'log_error',
            'args': [self.jid, msg]})
        self.send_request(msg)

    def finish(self):
        """log job complete with server"""
        msg = {'action': 'update_job_status', 'args': [self.jid, 5]}
        self.send_request(msg)
        try:
            self.usage = sge.qstat_usage(self.jid)[self.jid]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'update_job_usage',
                'args': [self.jid],
                'kwargs': kwargs}
            self.send_request(msg)
        except Exception as e:
            print(e)

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.send_request(msg)
        return response


class Manager(Client):
    """client node with methods to start/stop server"""

    def stop_server(self):
        """stop a running server"""
        response = self.send_request('stop')
        print(response[1])

    def isalive(self):
        """check whether server is alive.

        Returns:
            boolean True of False if it is alive."""

        # will return false here if no monitor_info.json exists
        try:
            self.connect()
        except IOError:
            return False

        # next we ping to see if server is alive under the monitor_info.json
        r = self.send_request({"action": "alive", "args": ""})
        if isinstance(r, tuple):
            if r[0] == 0:
                return True
            else:
                return False
        else:
            return False

    def start_server(self, prepend_to_path, conda_env, restart=False,
                     nolock=False):
        """Start new server instance

        Args:
            prepend_to_path (string): anaconda bin to prepend to path
            conda_env (string): python >= 3.5 conda env to run server in.
            restart (bool, optional): whether to force a new server instance to
                start. Will shutdown existing server instance if one exists.
            nolock (bool, optional): ignore any boot locks for the specified
                directory. Highly not recommended.

        Returns:
            Boolean whether the server started successfully or not.
        """
        # check if there is already a server here
        if self.isalive():
            if not restart:
                raise ServerRunning("server is already alive")
            else:
                print("server is already alive. will stop previous server.")
                self.stop_server()

        # check if there is a start lock in the file system.
        if os.path.isfile(self.out_dir + "/start.lock"):
            if not nolock:
                raise ServerStartLocked(
                    "server is already starting. If this is not the case "
                    "either remove 'start.lock' from server directory or use "
                    "option 'force'")
            else:
                warnings.warn("bypassing startlock. not recommended!!!!")

        # Pop open a new server instance on current node.
        open(self.out_dir + "/start.lock", 'w').close()
        shell = sge.true_path(executable="env_submit_master.sh")
        prepend_to_path = sge.true_path(file_or_dir=prepend_to_path)
        subprocess.Popen([shell, prepend_to_path, conda_env,
                         "launch_monitor.py", self.out_dir])
        time.sleep(15)
        os.remove(self.out_dir + "/start.lock")

        # check if it booted properly
        if self.isalive():
            print("successfully started server")
            return True
        else:
            print("server failed to start successfully")
            return False


class ManageJobMonitor(Manager):

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.send_request(msg)
        return response
