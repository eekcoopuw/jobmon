from __future__ import print_function

import os
from subprocess import CalledProcessError

from jobmon import sge, job
from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.executors import base


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

    remoterun = os.path.abspath(__file__)

    def __init__(self, mon_dir, request_retries, request_timeout,
                 path_to_conda_bin_on_target_vm, conda_env, parallelism=None):
        """
            path_to_conda_bin_on_target_vm (string, optional): which conda bin
                to use on the target vm.
            conda_env (string, optional): which conda environment you are
                using on the target vm.
        """

        super(SGEExecutor, self).__init__(
            mon_dir, request_retries, request_timeout, parallelism)

        # environment for distributed applications
        self.path_to_conda_bin_on_target_vm = path_to_conda_bin_on_target_vm
        self.conda_env = conda_env

    def execute_async(self, job, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            job (job.Job): instance of a job.Job

            see *args and **kwargs are passed to sge.qsub.

        Returns:
            sge job id
        """
        base_params = [
            "--mon_dir", self.mon_dir,
            "--runfile", job.runfile,
            "--jid", job.jid,
            "--request_retries", self.request_retries,
            "--request_timeout", self.request_timeout
        ]

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in job.job_args:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if passed_params:
            parameters = base_params + ["--pass_through",
                                        '"{}"'.format(" ".join(passed_params))]
        else:
            parameters = base_params

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

    def sync(self):

        # check state of all jobs currently in sge queue
        self.logger.debug('{}: Polling jobs...'.format(os.getpid()))
        current_jobs = set(sge.qstat(jids=self.running_jobs).job_id.tolist())
        self.logger.debug('             ... ' +
                          str(len(current_jobs)) + ' active jobs')
        self.running_jobs = list(current_jobs)


if __name__ == "__main__":
    import sys
    import argparse
    import subprocess
    import traceback

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    # for sge logging of standard error
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--mon_dir", required=True)
    parser.add_argument("--runfile", required=True)
    parser.add_argument("--jid", required=True, type=int)
    parser.add_argument("--request_timeout", required=True, type=int)
    parser.add_argument("--request_retries", required=True, type=int)
    parser.add_argument("--pass_through", required=False)
    args = vars(parser.parse_args())

    # reset sys.argv as if this parsing never happened
    passed_params = []
    for param in args["pass_through"].split():
        passed_params.append(str(param).replace("##", "--", 1))
    sys.argv = [args["runfile"]] + passed_params

    # start monitoring
    j1 = SGEJobInstance(args["mon_dir"], jid=args["jid"],
                        request_retries=args["request_retries"],
                        request_timeout=args["request_timeout"])
    j1.log_started()

    # open subprocess
    try:
        # TODO: discuss whether this should use the same Popen strategy as
        # LocalExecutor
        out = subprocess.check_output(["python"] + sys.argv,
                                      stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        eprint(exc.output)
        j1.log_job_stats()
        j1.log_error(str(exc.output))
        j1.log_failed()
    except:
        tb = traceback.format_exc()
        eprint(tb)
        j1.log_job_stats()
        j1.log_error(tb)
        j1.log_failed()
    else:
        print(out)
        j1.log_job_stats()
        j1.log_completed()
