from __future__ import print_function

import sys
import os
import json
import jsonpickle

from jobmon import sge, job
from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.executors import base
from jobmon.exceptions import ReturnCodes

if sys.version_info > (3, 0):
    import subprocess
    from subprocess import CalledProcessError
else:
    import subprocess32 as subprocess
    from subprocess32 import CalledProcessError


class SGEJobInstance(job._AbstractJobInstance):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        mon_dir (string): file path where the server configuration is
            stored.
        jid (int, optional): job id to use when communicating with
            jobmon database. If job id is not specified, will register as a new
            job and aquire the job id from the central job monitor.
        request_retries (int, optional): How many times to attempt to contact
            the central job monitor. Default=3
        request_timeout (int, optional): How long to wait for a response from
            the central job monitor. Default=3 seconds
    """

    def __init__(self, mon_dir, jid=None, request_retries=3,
                 request_timeout=3000):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified."""

        self.requester = Requester(mon_dir, request_retries, request_timeout)

        # get sge_id and name from envirnoment
        self.job_instance_id = int(os.getenv("JOB_ID"))
        self.name = os.getenv("JOB_NAME")

        # Try to get job_details
        job_info = self._sge_job_info()
        self.runfile = job_info['script_file']
        self.job_args = job_info['job_args']

        # TODO: would like to deprecate this and require a jid but I know the
        # dalynator uses this behaviour
        if jid is None:
            j = job.Job(mon_dir, jid, self.name, self.runfile,
                        self.job_args, request_retries, request_timeout)
            self.jid = j.jid
        else:
            self.jid = jid
        self.register_with_monitor()

    def _sge_job_info(self):
        try:
            job_info = sge.qstat_details(
                self.job_instance_id)[self.job_instance_id]
        except (CalledProcessError, Exception):
            job_info = {'script_file': 'Not Available',
                        'args': 'Not Available'}
        for reqdkey in ['script_file', 'job_args']:
            if reqdkey not in job_info.keys():
                job_info[reqdkey] = 'Not Available'
        return job_info

    def log_job_stats(self):
        try:
            self.usage = sge.qstat_usage(
                self.job_instance_id)[self.job_instance_id]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'update_job_instance_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            self.requester.send_request(msg)
        except Exception as e:
            self.log_error(str(e))

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_job_instance',
               'kwargs': {'job_instance_id': self.job_instance_id,
                          'jid': self.jid,
                          'job_instance_type': self.job_instance_type}}
        r = self.requester.send_request(msg)
        return r[1]

    def log_started(self):
        """log job start with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.RUNNING]}
        self.requester.send_request(msg)

    def log_completed(self):
        """log job complete with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.COMPLETE]}
        self.requester.send_request(msg)

    def log_failed(self):
        """log job failure with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.FAILED]}
        self.requester.send_request(msg)

    def log_error(self, error_msg):
        """log job error with server"""
        msg = {
            'action': 'log_job_instance_error',
            'args': [self.job_instance_id, error_msg]}
        self.requester.send_request(msg)


class SGEExecutor(base.BaseExecutor):
    """SGEExecutor executes tasks remotely in parallel on an SGE cluster.

    Args:
        mon_dir (string): directory where the connection info for the central
            job monitor is written
        request_retries (int, optional): how many time to try when pushing
            updates to the central job monitor
        request_timeout (int, optional): how long to linger on the zmq socket
            when pushing updates to the central job monitor and waiting for a
            response
        parallelism (int, optional): how many parallel jobs to schedule at a
            time
        subscribe_to_job_state (bool, optional): whether to subscribe to job
            state updates from the central job monitor via a zmq socket.
    """

    remoterun = os.path.abspath(__file__)

    def __init__(self, mon_dir, request_retries=3, request_timeout=3000,
                 parallelism=None, subscribe_to_job_state=True):

        super(SGEExecutor, self).__init__(
            mon_dir, request_retries, request_timeout, parallelism)

        # environment for distributed applications
        conda_info = json.loads(
            subprocess.check_output(['conda', 'info', '--json']).decode())
        path_to_conda_bin_on_target_vm = '{}/bin'.format(
            conda_info['root_prefix'])
        conda_env = conda_info['default_prefix'].split("/")[-1]
        self.path_to_conda_bin_on_target_vm = path_to_conda_bin_on_target_vm
        self.conda_env = conda_env

    def execute_async(self, job, process_timeout=None, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            job (job.Job): instance of a job.Job
            process_timeout (int, optional): how many seconds to wait for a job
                to finish before killing it and registering a failure. Default
                is forever.

            *args and **kwargs are passed to sge.qsub.

        Returns:
            sge job id
        """
        parameters = [
            "--mon_dir", self.mon_dir,
            "--runfile", job.runfile,
            "--jid", job.jid,
            "--request_retries", self.request_retries,
            "--request_timeout", self.request_timeout,
            "--pass_through", "'{}'".format(jsonpickle.encode(job.job_args)),
            "--process_timeout", process_timeout
        ]

        self.logger.debug(
            ("{}: Submitting job to qsub:"
             " runfile {}; jobname {}; parameters {}; path: {}"
             ).format(os.getpid(),
                      self.remoterun,
                      job.name,
                      parameters,
                      self.path_to_conda_bin_on_target_vm))
        # submit.
        job_instance_id = sge.qsub(
            runfile=self.remoterun,
            jobname=job.name,
            prepend_to_path=self.path_to_conda_bin_on_target_vm,
            conda_env=self.conda_env,
            parameters=parameters,
            *args, **kwargs)

        # ideally would create an sge job instance here and return it instead
        # of the id
        return job_instance_id

    def flush_lost_jobs(self):
        """check for jobs currently in sge queue to make sure there
        are not any straglers that died with out registering with the
        central job monitor"""
        results = sge.qstat(jids=self.running_jobs).job_id.tolist()
        for jid in [j for j in self.running_jobs if j not in results]:
            self.jobs[jid]["status_id"] = Status.UNKNOWN


if __name__ == "__main__":
    import argparse

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    # for sge logging of standard error
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def jpickle_parser(s):
        return jsonpickle.decode(s)

    def intnone_parser(s):
        try:
            return int(s)
        except ValueError:
            return None

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--mon_dir", required=True)
    parser.add_argument("--runfile", required=True)
    parser.add_argument("--jid", required=True, type=int)
    parser.add_argument("--request_timeout", required=True, type=int)
    parser.add_argument("--request_retries", required=True, type=int)
    parser.add_argument("--process_timeout", required=True,
                        type=intnone_parser)
    parser.add_argument("--pass_through", required=False, type=jpickle_parser)
    args = vars(parser.parse_args())

    # reset sys.argv as if this parsing never happened
    sys.argv = [args["runfile"]] + args["pass_through"]

    # start monitoring with subprocesses pid
    job_instance = SGEJobInstance(
        args["mon_dir"],
        jid=args["jid"],
        request_retries=args["request_retries"],
        request_timeout=args["request_timeout"])
    job_instance.log_started()

    for arg in sys.argv:
        if not isinstance(arg, str) and not isinstance(arg, unicode):
            raise ValueError(
                "all command line arguments must be strings. {} is {}"
                .format(arg, type(arg)))
    try:
        # open subprocess
        proc = subprocess.Popen(
            ["python"] + [str(arg) for arg in sys.argv],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    except ValueError as err:
        # according to POPEN docs a ValueError is raised if popen is
        # called with inproper arguments in which case LocalJobInstance
        # was never initialized
        proc.kill()
        eprint(err)
    else:
            # communicate till done
        stdout, stderr = proc.communicate(timeout=args["process_timeout"])
        print(stdout)
        eprint(stderr)

        # check return code
        if proc.returncode != ReturnCodes.OK:
            job_instance.log_job_stats()
            job_instance.log_error(str(stderr))
            job_instance.log_failed()
        else:
            job_instance.log_job_stats()
            job_instance.log_completed()
