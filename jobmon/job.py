import os
import json
from subprocess import CalledProcessError

from jobmon import sge
from jobmon.requester import Requester
from jobmon.models import Status


class Job(object):
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

    def __init__(self, out_dir, name=None):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        self.name = name
        self.requester = Requester(out_dir)

        # get jid from monitor
        self.monitored_jid = self.register_with_monitor()

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_job', 'kwargs': {'name': self.name}}
        r = self.requester.send_request(msg)
        return r[1]

    def log_started(self):
        """log job start with server"""
        msg = {'action': 'update_job_status',
               'args': [self.monitored_jid, Status.RUNNING]}
        self.requester.send_request(msg)

    def log_failed(self):
        """log job failure with server"""
        msg = {'action': 'update_job_status',
               'args': [self.monitored_jid, Status.FAILED]}
        self.requester.send_request(msg)

    def log_error(self, error_msg):
        """log job error with server"""
        # msg = json.dumps({
        #     'action': 'log_error',
        #     'args': [self.monitored_jid, error_msg]})
        msg = {
            'action': 'log_error',
            'args': [self.monitored_jid, error_msg]}
        self.requester.send_request(msg)

    def log_completed(self):
        """log job complete with server"""
        msg = {'action': 'update_job_status',
               'args': [self.monitored_jid, Status.COMPLETE]}
        self.requester.send_request(msg)
        try:
            self.usage = sge.qstat_usage(self.sge_id)[self.sge_id]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'update_job_usage',
                'args': [self.sge_id],
                'kwargs': kwargs}
            self.requester.send_request(msg)
        except Exception as e:
            print(e)


class SGEJob(Job):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        out_dir (string): file path where the server configuration is
            stored.
        name (string, optional): name current process. If name is not specified
            will attempt to use environment variable JOB_NAME.
    """

    def __init__(self, out_dir):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified."""
        self.requester = Requester(out_dir)

        # get sge_id and name from envirnoment
        self.sge_id = os.getenv("JOB_ID")
        self.name = os.getenv("JOB_NAME")

        # Try to get job_details
        try:
            self.job_info = sge.qstat_details(self.sge_id)[self.sge_id]
        except CalledProcessError:
            self.job_info = {'script_file': 'Not Available',
                             'args': 'Not Available'}
        except Exception:
            self.job_info = {'script_file': 'Not Available',
                             'args': 'Not Available'}
        for reqdkey in ['script_file', 'job_args']:
            if reqdkey not in self.job_info.keys():
                self.job_info[reqdkey] = 'Not Available'
        self.monitored_jid = self.register_with_monitor()

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_sgejob',
               'kwargs': {'name': self.name,
                          'sge_id': self.sge_id}}
        r = self.requester.send_request(msg)
        return r[1]
