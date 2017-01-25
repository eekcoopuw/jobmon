import logging
import os
import time

from jobmon import requester, sge
from jobmon.exceptions import (CannotConnectToCentralJobMonitor,
                               CentralJobMonitorNotAlive)

here = os.path.dirname(os.path.abspath(__file__))


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
        sgeid = sge.qsub(*args, **kwargs)
        self.logger.debug(
            "Submitting job, sgeid {}: {} {}".format(sgeid, args, kwargs))
        self.scheduled_jobs.append(sgeid)
        self.jobs[sgeid] = {"args": args, "kwargs": kwargs}
        return sgeid

    def qblock(self, poll_interval=10):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. check sge queue each poll_interval until q is
        clear. Run qmanage each poll_interval.

        Args:
            poll_interval (int, optional): time in seconds between each
                qcomplete()
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
        self.logger.debug('             ... ' +
                          str(len(current_jobs)) + ' active jobs')
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

    def __init__(self, mon_dir, path_to_conda_bin_on_target_vm,
                 conda_env, max_alive_wait_time=45, request_retries=3,
                 request_timeout=30000):
        """
        Args:
            mon_dir (string): directory where monitor server is running
            path_to_conda_bin_on_target_vm (string, optional): which conda bin
                to use on the target vm.
            conda_env (string, optional): which conda environment you are
                using on the target vm.
            max_alive_wait_time (int, optional): how long to wait for an alive
                signal from the central job monitor
            request_retries (int, optional): how many times to attempt to
                communicate with the central job monitor per request
            request_timeout (int, optional): how long till each communication
                attempt times out
        """
        super(MonitoredQ, self).__init__()
        self.mon_dir = mon_dir

        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

        # environment for distributed applications
        self.path_to_conda_bin_on_target_vm = path_to_conda_bin_on_target_vm
        self.conda_env = conda_env
        self.wrapperfile = os.path.join(here, "monitored_job.py")

        # communication settings
        self.request_retries = request_retries
        self.request_timeout = request_timeout

        # connect requester instance to central job monitor
        self.request_sender = requester.Requester(self.mon_dir)
        if not self.request_sender.is_connected():
            raise CannotConnectToCentralJobMonitor(
                "unable to connect to central job monitor in {}".format(
                    self.mon_dir, self.request_retries, self.request_timeout))

        # make sure server is alive
        time_spent = 0
        while not self._central_job_monitor_alive():
            if time_spent > max_alive_wait_time:
                msg = ("unable to confirm central job monitor is alive in {}."
                       " Maximum boot time exceeded: {}").format(
                           self.mon_dir, max_alive_wait_time)
                self.logger.debug(msg)
                raise CentralJobMonitorNotAlive(msg)
                break

            # sleep and increment
            time.sleep(5)
            time_spent += 5

    def _central_job_monitor_alive(self):
        try:
            resp = self.request_sender.send_request({"action": "alive"})
        except Exception as e:
            self.logger.error('Error sending request', exc_info=e)
            # catch some class of errors?
            resp = [0, u"No, not alive"]

        # check response
        if resp[0] == 0:
            return True
        else:
            return False

    def qsub(self, runfile, jobname, monitored_jid=None, parameters=[],
             *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            runfile (sting): full path to python executable file.
            jobname (sting): what name to register the sge job under.
            monitored_jid (int, optional): what id to use for this job in the
                jobmon database. by default will auto increment using get_i().
            parameters (list, optional): command line arguments to be passed
                into runfile.

            see sge.qsub for further options

        Returns:
            sge job id
        """

        # configure arguments for parsing by ./bin/monitored_job.py
        if monitored_jid is None:
            msg = {'action': 'register_job',
                   'kwargs': {
                       'name': jobname,
                       'runfile': runfile,
                       'args': ','.join(str(parameters)
                                        ) if parameters else None}}
            r = self.request_sender.send_request(msg)
            monitored_jid = r[1]

        if not isinstance(monitored_jid, int):
            raise Exception(
                "Could not create job, monitored_jid = '{}'".format(
                    monitored_jid))
        base_params = [
            "--mon_dir", self.mon_dir,
            "--runfile", runfile,
            "--monitored_jid", monitored_jid,
            "--request_retries", self.request_retries,
            "--request_timeout", self.request_timeout
        ]

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in parameters:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if parameters:
            parameters = base_params + passed_params
        else:
            parameters = base_params

        self.logger.debug(
            ("{}: Submitting job to qsub:"
             " runfile {}; jobname {}; parameters {}; path: {}"
             ).format(os.getpid(),
                      self.wrapperfile,
                      jobname,
                      parameters,
                      self.path_to_conda_bin_on_target_vm))
        # submit.
        sgeid = sge.qsub(
            runfile=self.wrapperfile,
            jobname=jobname,
            prepend_to_path=self.path_to_conda_bin_on_target_vm,
            conda_env=self.conda_env,
            parameters=parameters,
            *args, **kwargs)

        # store submission params in self.jobs dict in case of resubmit
        self.scheduled_jobs.append(sgeid)
        self.jobs[monitored_jid] = {
            "runfile": runfile,
            "jobname": jobname,
            "parameters": passed_params,
            "args": args,
            "kwargs": kwargs}
        return sgeid
