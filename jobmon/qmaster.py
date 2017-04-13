import logging
import time
from threading import Thread, Event
from timeit import default_timer as timer

from jobmon import requester
from jobmon.job import Job
from jobmon.exceptions import (CannotConnectToCentralJobMonitor,
                               CentralJobMonitorNotAlive)


class JobQueue(object):
    """Queue supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    _keep_alive = True

    def __init__(self, executor, max_alive_wait_time=45):
        """
        Args:
            mon_dir (str): directory where monitor server is running
            max_alive_wait_time (int, optional): how long to wait for an alive
                signal from the central job monitor
        """
        self.logger = logging.getLogger(__name__)

        self.jobs = {}
        self.executor = executor

        # schedular attributes
        self._thread_stop_request = None
        self._scheduler_thread = None

        # connect requester instance to central job monitor
        self.request_sender = requester.Requester(
            self.executor.mon_dir, self.executor.request_retries,
            self.executor.request_timeout)
        if not self.request_sender.is_connected():
            raise CannotConnectToCentralJobMonitor(
                "unable to connect to central job monitor in {}".format(
                    self.executor.mon_dir))

        # make sure server is alive
        time_spent = 0
        while not self._central_job_monitor_alive():
            if time_spent > max_alive_wait_time:
                msg = ("unable to confirm central job monitor is alive in {}."
                       " Maximum boot time exceeded: {}").format(
                           self.executor.mon_dir, max_alive_wait_time)
                self.logger.debug(msg)
                raise CentralJobMonitorNotAlive(msg)
                break

            # sleep and increment
            time.sleep(5)
            time_spent += 5

    @property
    def keep_alive(self):
        if self._thread_stop_request:
            if self._thread_stop_request.isSet():
                self._keep_alive = False
        return self._keep_alive

    @keep_alive.setter
    def keep_alive(self, val):
        self._keep_alive = val

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
        self.jobs[job.jid] = job
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

    def _schedule(self, flush_lost_jobs_interval):
        start = timer()
        while self.keep_alive:
            # check if we want to
            check_lost_jobs = (timer() - start) > flush_lost_jobs_interval
            if check_lost_jobs:
                start = timer()

            # update all queues
            self.executor.refresh_queues(flush_lost_jobs=check_lost_jobs)

    def stop_scheduler(self):
        """stop scheduler if it is being run in a thread"""
        if self._scheduler_thread is not None:
            self._thread_stop_request.set()
            self._scheduler_thread.join(timeout=10)

    def run_scheduler(self, async=True, flush_lost_jobs_interval=60, *args,
                      **kwargs):
        """Continuously poll for job status updates and schedule any jobs if
        there are open resources

        Args:
            async (bool, optional): whether to run the scheduler asynchronously
                in a thread
            flush_lost_jobs_interval (int, optional): how frequently to call
                the flush_lost_jobs() method. This method tends to be expensive
                so is only called intermittently

            *args and **kwargs are passed to the underlying _schedule() method
        """
        if async:
            self._thread_stop_request = Event()
            self._scheduler_thread = Thread(
                target=self._schedule, args=([flush_lost_jobs_interval]))
            self._scheduler_thread.start()
        else:
            self._schedule(flush_lost_jobs_interval=60)

    def block_till_done(self, poll_interval=60):
        """continuously queue until all jobs have finished

        Args:
            poll_interval (int, optional): how frequently to call the heartbeat
                method which updates the queue.
        """
        self.run_scheduler()
        while (len(self.executor.queued_jobs) > 0 or
               len(self.executor.running_jobs) > 0):
            time.sleep(poll_interval)
        self.stop_scheduler()


class RetryJobQueue(JobQueue):

    def __init__(self, *args, **kwargs):
        super(RetryJobQueue, self).__init__(*args, **kwargs)

        self._retry_limit_exceeded = []

    def _schedule(self, flush_lost_jobs_interval, retries=1):
        start = timer()
        while self.keep_alive:

            # check for failures and reset their status
            reschedule = (
                set(self.executor.failed_jobs) -
                set(self._retry_limit_exceeded))
            for jid in reschedule:
                tries = len(self.executor.jobs[jid]["job"].job_instance_ids)

                if tries <= retries:
                    self.logger.info("job {} failed. Try... #{}".format(
                        jid, tries + 1))
                    self.executor.jobs[jid]["status_id"] = None
                else:
                    self._retry_limit_exceeded.append(jid)

            # check if we want to
            check_lost_jobs = (timer() - start) > flush_lost_jobs_interval
            if check_lost_jobs:
                start = timer()

            # update all queues
            self.executor.refresh_queues(flush_lost_jobs=check_lost_jobs)
