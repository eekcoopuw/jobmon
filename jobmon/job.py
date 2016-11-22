
import os
import json
from logging import Handler
from subprocess import CalledProcessError
from jobmon.sender import Sender
from . import sge


class ServerRunning(Exception):
    pass


class ServerStartLocked(Exception):
    pass


def log_exceptions(job):
    def wrapper(func):
        def catch_and_send(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
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


class Job(Sender):
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
    def __init__(self, out_dir, jid=None, name=None, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        super(Job, self).__init__(out_dir, **kwargs)

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
        self.usage = None

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
            self.usage = sge.qstat_usage(self.sge_id)[self.sge_id]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'update_job_usage',
                'args': [self.sge_id],
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
