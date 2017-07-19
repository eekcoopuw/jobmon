import logging
import time

from jobmon import requester
from jobmon.connection import CentralMonitorConnection
from jobmon.schedulers import SimpleScheduler
from jobmon.executors.local_exec import LocalExecutor
from jobmon.job import Job
from jobmon.exceptions import (CannotConnectToCentralJobMonitor,
                               CentralJobMonitorNotAlive)


class JobQueue(object):
    """Queue supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    def __init__(self, mon_dir="", monitor_connection=None, executor=LocalExecutor, executor_params={},
                 scheduler=SimpleScheduler, scheduler_params={},
                 max_alive_wait_time=45):
        """
        Args:
            mon_dir (str): if  the central job monitor is running locally then this is
                the directory where it is running.
            monitor_connection: If the CJM is running remotely, then this is connection information.
            executor (obj, optional): LocalExecutor or SGEExecutor
            executor_params (dict, optional): kwargs to be passed into executor
                instantiation.
            scheduler (obj, optional): Simplescheduler or Retryscheduler
            scheduler_params (dict, optional): kwargs to be passed into
                scheduler instantiation.
            max_alive_wait_time (int, optional): how long to wait for an alive
                signal from the central job monitor
        """
        self.logger = logging.getLogger(__name__)
        if not (bool(mon_dir) ^ bool(monitor_connection)):
            raise ValueError("Either mon_dir or the combination monitor_host+"
                             "monitor_port must be specified. Cannot specify "
                             "both mon_dir and a host+port pair.")

        self.executor = executor(mon_dir=mon_dir, **executor_params)
        self.scheduler = scheduler(executor=self.executor, **scheduler_params)

        # connect requester instance to central job monitor
        self.request_sender = requester.Requester(out_dir=mon_dir, monitor_connection=monitor_connection)
        if not self.request_sender.is_connected():
            raise CannotConnectToCentralJobMonitor(
                "unable to connect to central job monitor, dir= {d}, connection config = {cc}".format(
                    d=self.executor.mon_dir, cc = monitor_connection))

        # make sure server is alive
        time_spent = 0
        while not self.central_job_monitor_alive():
            if time_spent > max_alive_wait_time:
                msg = ("unable to confirm central job monitor is alive in {}."
                       " Maximum boot time exceeded: {}").format(
                           self.executor.mon_dir, max_alive_wait_time)
                self.logger.debug(msg)
                raise CentralJobMonitorNotAlive(msg)

            # sleep and increment
            time.sleep(5)
            time_spent += 5

    def central_job_monitor_alive(self):
        """check whether the central job monitor is running"""
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

    def scheduler_alive(self):
        """check whether the scheduler is running"""
        return self.scheduler.is_alive()

    def create_job(self, runfile, jobname, parameters=[]):
        """create a new job record in the central database and on this Q
        instance

        Args:
            runfile (str): full path to python executable file.
            jobname (str): what name to register the sge job under.
            parameters (list, optional): command line arguments to be passed
                into runfile.

        """
        job = Job(self.executor.mon_dir, name=jobname, runfile=runfile,
                  job_args=parameters)
        return job

    def queue_job(
            self, job, process_timeout=None, *args, **kwargs):
        """queue a job in the executor

        Args:
            job (jobmon.job.Job): instance of jobmon job.
            process_timeout (int, optional): time in seconds to wait for
                process to finish. default is forever

            args and kwargs are passed through to the executors exec_async
            method
        """
        self.executor.queue_job(job, process_timeout=None, *args, **kwargs)

    def run_scheduler(self, *args, **kwargs):
        """Continuously poll for job status updates and schedule any jobs if
        there are open resources

        *args and **kwargs are passed to scheduler.run_scheduler()
        """
        self.scheduler.run_scheduler(*args, **kwargs)

    def stop_scheduler(self):
        """stop scheduler if it is being run in a thread"""
        self.scheduler.stop_scheduler()

    def block_till_done(self, poll_interval=60, stop_scheduler_when_done=True,
                        *args, **kwargs):
        """continuously queue until all jobs have finished

        Args:
            poll_interval (int, optional): how frequently to check whether the
                executor has finished all jobs.
            stop_scheduler_when_done (bool, optional): whether to close the
                scheduler thread when all jobs are finished executing

            *args and **kwargs are passed to scheduler.run_scheduler()
        """
        if not self.scheduler_alive():
            self.run_scheduler(*args, **kwargs)
        while (len(self.executor.queued_jobs) > 0 or
               len(self.executor.running_jobs) > 0):
            time.sleep(poll_interval)
        if stop_scheduler_when_done:
            self.stop_scheduler()
