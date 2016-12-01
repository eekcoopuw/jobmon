import logging
import os
import time
import warnings

import jobmon.job
import jobmon.sge as sge
from jobmon.central_job_monitor_launcher import CentralJobMonitorLauncher, CentralJobMonitorRunning, \
    CentralJobMonitorStartLocked

this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))


class IgnorantQ(object):
    """keep track of jobs submitted to sun grid engine"""

    def __init__(self):
        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}
        self.logger = logging.getLogger(__name__)

    def qsub(self, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. see sge module for
        documentation.

        Returns:
            sge job id
        """
        sgeid = jobmon.sge.qsub(*args, **kwargs)
        self.logger.debug("Submitting job, sgeid {}: {} {}".format(sgeid, args, kwargs))
        self.scheduled_jobs.append(sgeid)
        self.jobs[sgeid] = {"args": args, "kwargs": kwargs}
        return sgeid

    def qblock(self, poll_interval=10):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. check sge queue each poll_interval until q is
        clear. Run qmanage each poll_interval.

        Args:
            poll_interval (int, optional): time in seconds between each qcomplete()
        """
        self.logger.debug("qblock entered")
        while not self.qcomplete():
            self.logger.debug("qblock sleeping")
            time.sleep(poll_interval)
        self.logger.debug("qblock complete")

    def qmanage(self):
        """run manage_exit_q and manage_current_q based on the changes to those
        queues between the current qmanage call and the previous call"""
        self.logger.debug('{}: Polling jobs...'.format(os.getpid()))

        # manage all jobs currently in sge queue
        current_jobs = set(sge.qstat(jids=self.scheduled_jobs
                                     ).job_id.tolist())
        self.logger.debug('             ... ' + str(len(current_jobs)) + ' active jobs')
        self.manage_current_q(current_jobs)

        # manage each job that has left the sge q
        scheduled_jobs = set(self.scheduled_jobs)
        exit_jobs = list(scheduled_jobs - current_jobs)
        for sgeid in exit_jobs:
            self.scheduled_jobs.remove(sgeid)
        self.manage_exit_q(exit_jobs)

    def qcomplete(self):
        """check if qmaster instance has done all possible work currently"""
        self.qmanage()
        return len(self.scheduled_jobs) == 0

    def manage_exit_q(self, exit_jobs):
        """action to take when job has left the q. delete job from jobs dict
        del(self.jobs[jid])

        Args:
            exit_jobs (list): list of sge job ids that have left the queue
        """
        pass

    def manage_current_q(self, current_jobs):
        """action to take on any job in the sge queue. For ignorant q, there
        is no management by default. override to add custom handling

        Args:
            current_jobs (set): set of sge job ids that are currently in the
                sge queue
        """
        pass


# TODO: Study and determine whether this class can be used as a basis or
# at least a template for tracking state of a "DAG" of jobs, and preferably
# for handling submission+retry logic for said jobs.
class MonitoredQ(IgnorantQ):
    """monitored Q supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    def __init__(self, out_dir, path_to_conda_bin_on_target_vm,
                 conda_env, request_timeout=10000, request_retries=3, maximum_boot_sleep_time=45):
        """
        Args:
            out_dir (string): directory where monitor server is running
            path_to_conda_bin_on_target_vm (string, optional): which conda bin you are using.
                only use if MonitoredQ can't figure it out on it's own.
            conda_env (string, optional): which conda environment you are
                using. only use if MonitoredQ can't figure it out on it's own.
            request_retries (int, optional): how many times to resubmit failed jobs
        """
        super(MonitoredQ, self).__init__()
        self.out_dir = out_dir
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.boot_sleep_time = maximum_boot_sleep_time

        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

        # environment for distributed applications

        self.path_to_conda_bin_on_target_vm = path_to_conda_bin_on_target_vm
        self.conda_env = conda_env
        self.runfile = "monitored_job.py"

        # internal server central_job_monitor_launcher
        self.central_job_monitor_launcher = CentralJobMonitorLauncher(
            out_dir, request_retries=self.request_retries,
            request_timeout=self.request_timeout)

        # make sure server is booted
        # Use a busy wait loop with a maximum time out out
        time_spent = 0
        while not self.central_job_monitor_launcher.is_alive():
            try:
                self.start_central_monitor(conda_env=conda_env,
                                           path_to_conda_bin_on_target_vm=path_to_conda_bin_on_target_vm)
            except CentralJobMonitorRunning:
                self.logger.debug("Central Job Monitor is running after {} seconds".format(time_spent))
                break
            except CentralJobMonitorStartLocked:
                self.logger.debug("Start lock is present, sleeping for {} seconds".format(maximum_boot_sleep_time))
            time.sleep(5)
            time_spent += 5
            if time_spent > maximum_boot_sleep_time:
                if not self.central_job_monitor_launcher.is_alive():
                    warnings.warn("QMaster could not start the CentralJobMonitorLauncher")
                    # TODO Give up completely? raise an exception?
                    break
                else:
                    self.logger.debug("Central Job Monitor is running after {} seconds".format(time_spent))
                    break
                    # Strictly No need for else - if it is running the except CentralJobMonitorRunning will catch it,
                    # but it feels dangerous not to have an else and a break

    def start_central_monitor(self,
                              path_to_conda_bin_on_target_vm="/ihme/code/central_comp/anaconda/bin",
                              conda_env="jobmon35", restart=False, nolock=False):
        """Start a CentralMonitor server in a subprocess. MonitoredQ's share
        monitor servers and auto increments if they are initialized with the
        same out_dir in the same python instance.

        Args:
            path_to_conda_bin_on_target_vm (string, optional): anaconda bin to prepend to path
            conda_env (string, optional): python >= 3.5 conda env to run server
                in.
            restart (bool, optional): whether to force a new server instance to
                start. Will shutdown existing server instance if one exists.
            nolock (bool, optional): ignore any boot locks for the specified
                directory. Highly not recommended.

        Returns:
            Boolean whether the server started successfully or not.
        """
        path_to_conda_bin_on_target_vm = sge.true_path(file_or_dir=path_to_conda_bin_on_target_vm)
        self.central_job_monitor_launcher.start_server(path_to_conda_bin_on_target_vm=path_to_conda_bin_on_target_vm,
                                                       conda_env=conda_env, restart=restart,
                                                       nolock=nolock)

    def stop_monitor(self):
        """stop jobmonitor server tied to this MonitoredQ instance"""
        if self.central_job_monitor_launcher.is_alive():
            self.central_job_monitor_launcher.stop_server()

    def qsub(self, runfile, jobname, jid=None, parameters=[], *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            runfile (sting): full path to python executable file.
            jobname (sting): what name to register the sge job under.
            jid (int, optional): what id to use for this job in the
                jobmon database. by default will auto increment using get_i().
            parameters (list, optional): command line arguments to be passed
                into runfile.

            see sge.qsub for further options

        Returns:
            sge job id
        """

        # configure arguments for parsing by ./bin/monitored_job.py
        if jid is None:
            msg = {'action': 'create_job', 'args': ''}
            r = self.central_job_monitor_launcher.requester.send_request(msg)
            jid = r[1]

        if not isinstance( jid, int):
            raise "Could not create job, jid = '{}'".format(jid
                                                            )
        base_params = ["--mon_dir", self.out_dir, "--runfile", runfile,
                       "--jid", jid]
        if self.request_timeout is not None:
            base_params.extend(["--request_timeout", self.request_timeout])
        if self.request_retries is not None:
            base_params.extend(["--request_retries", self.request_retries])

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in parameters:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if parameters:
            parameters = base_params + passed_params
        else:
            parameters = base_params

        self.logger.debug("{}: Submitting job to qsub:"
                          " runfile {}; jobname {}; parameters {}; path: {}".format(os.getpid(),
                                                                                    self.runfile,
                                                                                    jobname,
                                                                                    parameters,
                                                                                    self.path_to_conda_bin_on_target_vm))
        # submit.
        sgeid = sge.qsub(runfile=self.runfile, jobname=jobname,
                         prepend_to_path=self.path_to_conda_bin_on_target_vm,
                         conda_env=self.conda_env,
                         jobtype=None, parameters=parameters, *args, **kwargs)

        # update database to reflect submitted status
        msg = {'action': 'update_job_status', 'args': [jid, 2]}
        self.central_job_monitor_launcher.requester.send_request(msg)

        # store submission params in self.jobs dict in case of resubmit
        self.scheduled_jobs.append(sgeid)
        self.jobs[jid] = {"runfile": runfile,
                          "jobname": jobname,
                          "parameters": parameters,
                          "args": args,
                          "kwargs": kwargs}
        return sgeid
