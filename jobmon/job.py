import os
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
        monitored_jid (int, optional): job id to use when communicating with
            jobmon database. If job id is not specified, will register as a new
            job and aquire the job id from the central job monitor.
        name (string, optional): name current process. If name is not specified
            will default to None.
    """

    def __init__(self, out_dir, monitored_jid=None, name=None, runfile=None,
                 job_args=None, *args, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        self.requester = Requester(out_dir, *args, **kwargs)

        # get jid from monitor
        self.monitored_jid = monitored_jid
        self.name = name
        self.runfile = runfile
        self.job_args = job_args

        if self.monitored_jid is None:
            self.monitored_jid = self.register_with_monitor()

        self.job_instances = []

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_job',
               'kwargs': {'name': self.name,
                          'runfile': self.runfile,
                          'job_args': self.job_args}}
        r = self.requester.send_request(msg)
        return r[1]


class SGEJob(object):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        out_dir (string): file path where the server configuration is
            stored.
        name (string, optional): name current process. If name is not specified
            will attempt to use environment variable JOB_NAME.
    """

    def __init__(self, out_dir, monitored_jid=None, *args, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified."""
        self.requester = Requester(out_dir, *args, **kwargs)

        # get sge_id and name from envirnoment
        self.sge_id = int(os.getenv("JOB_ID"))
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
        self.runfile = self.job_info['script_file']
        self.job_args = self.job_info['job_args']

        if monitored_jid is None:
            self.monitored_jid = super(SGEJob, self).register_with_monitor()
        else:
            self.monitored_jid = monitored_jid
        self.register_with_monitor()

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_sgejob',
               'kwargs': {'sge_id': self.sge_id,
                          'monitored_jid': self.monitored_jid}}
        r = self.requester.send_request(msg)
        return r[1]

    def log_job_stats(self):
        try:
            self.usage = sge.qstat_usage(self.sge_id)[self.sge_id]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'update_sgejob_usage',
                'args': [self.sge_id],
                'kwargs': kwargs}
            self.requester.send_request(msg)
        except Exception as e:
            self.log_error(str(e))

    def log_started(self):
        """log job start with server"""
        msg = {'action': "update_sgejob_status",
               'args': ["sge_id", Status.RUNNING]}
        self.requester.send_request(msg)

    def log_completed(self):
        """log job complete with server"""
        msg = {'action': "update_sgejob_status",
               'args': ["sge_id", Status.COMPLETE]}
        self.requester.send_request(msg)

    def log_failed(self):
        """log job failure with server"""
        msg = {'action': "update_sgejob_status",
               'args': ["sge_id", Status.FAILED]}
        self.requester.send_request(msg)

    def log_error(self, error_msg):
        """log job error with server"""
        msg = {
            'action': 'log_sge_job_error',
            'args': [self.sge_id, error_msg]}
        self.requester.send_request(msg)
