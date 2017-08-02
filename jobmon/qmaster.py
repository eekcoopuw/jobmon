import logging
import time

from jobmon import requester
from jobmon.connection_config import ConnectionConfig
from jobmon.schedulers import SimpleScheduler
from jobmon.executors.local_exec import LocalExecutor
from jobmon.job import Job
from jobmon.exceptions import (CannotConnectToCentralJobMonitor,
                               CentralJobMonitorNotAlive)

from jobmon.setup_logger import setup_logger

setup_logger("jobmon", path="client_logging.yaml)")

logger = logging.getLogger(__name__)

class JobQueue(object):
    """Queue supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    def __init__(self, monitor_connection=None, publisher_connection=None, executor=LocalExecutor, executor_params={},
                 scheduler=SimpleScheduler, scheduler_params={},
                 max_alive_wait_time=45):
        """
        Args:
            monitor_connection (ConnectionConfig): host, port, timeout and retry parameters
                of the central job monitor
            executor (obj, optional): LocalExecutor or SGEExecutor
            executor_params (dict, optional): kwargs to be passed into executor
                instantiation.
            scheduler (obj, optional): SimpleScheduler or RetryScheduler
            scheduler_params (dict, optional): kwargs to be passed into
                scheduler instantiation.
            max_alive_wait_time (int, optional): how long to wait for an alive
                signal from the central job monitor
        """

        self.executor = executor(monitor_connection=monitor_connection, publisher_connection=publisher_connection, **executor_params)
        self.scheduler = scheduler(executor=self.executor, **scheduler_params)

        # connect requester instance to central job monitor
        self.request_sender = requester.Requester(monitor_connection=monitor_connection)
        if not self.request_sender.is_connected():
            raise CannotConnectToCentralJobMonitor(
                "unable to connect to central job monitor, dir= {d}, connection config = {cc}".format(
                    d=self.executor.monitor_connection, cc=monitor_connection))

        # make sure server is alive
        time_spent = 0
        while not self.central_job_monitor_alive():
            if time_spent > max_alive_wait_time:
                msg = ("Unable to confirm central job monitor is alive in {}."
                       " Maximum boot time exceeded: {}").format(
                           self.executor.monitor_connection, max_alive_wait_time)
                logger.debug(msg)
                raise CentralJobMonitorNotAlive(msg)

            # sleep and increment
            time.sleep(5)
            time_spent += 5

    def central_job_monitor_alive(self):
        """check whether the central job monitor is running"""
        try:
            resp = self.request_sender.send_request({"action": "alive"})
        except Exception as e:
            logger.error('Error sending request', exc_info=e)
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
        job = Job(self.executor.monitor_connection, name=jobname, runfile=runfile,
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
        logger.debug("Queueing job {}".format(job.name))
        self.executor.queue_job(job, process_timeout=process_timeout, *args,
                                **kwargs)

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
                        operator_message="",
                        *args, **kwargs):
        """continuously queue until all jobs have finished

        Args:
            poll_interval (int, optional): how frequently to check whether the
                executor has finished all jobs.
            stop_scheduler_when_done (bool, optional): whether to close the
                scheduler thread when all jobs are finished executing

            *args and **kwargs are passed to scheduler.run_scheduler()
        """
        logger.info("Blocking, poll interval = {}".format(poll_interval))
        if not self.scheduler_alive():
            self.run_scheduler(*args, **kwargs)
        while (len(self.executor.queued_jobs) > 0 or
               len(self.executor.running_jobs) > 0):
            logger.info("Jobmon waiting: {} Queued Jobs: {}, {} Running Jobs: {}".format(
                len(self.executor.queued_jobs),
                [str(x) for x in self.executor.queued_job_objects],
                len(self.executor.running_jobs),
                [str(x) for x in self.executor.running_job_objects]))
            time.sleep(poll_interval)
        if stop_scheduler_when_done:
            self.stop_scheduler()
