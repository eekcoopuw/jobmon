import abc
import logging
from threading import Thread, Event
from timeit import default_timer as timer


ABC = abc.ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class _BaseScheduler(ABC):

    _keep_alive = True

    def __init__(self, executor):

        self.logger = logging.getLogger(__name__)

        self.executor = executor

        # scheduler attributes
        self._thread_stop_request = None
        self._scheduler_thread = None

    @property
    def keep_alive(self):
        if self._thread_stop_request:
            if self._thread_stop_request.isSet():
                self._keep_alive = False
        return self._keep_alive

    @keep_alive.setter
    def keep_alive(self, val):
        self._keep_alive = val

    @abc.abstractmethod
    def _schedule(self):
        return

    def is_alive(self):
        """is scheduler currently running?"""
        if self._scheduler_thread:
            return True
        else:
            return False

    def stop_scheduler(self):
        """stop scheduler if it is being run in a thread"""
        if self._scheduler_thread is not None:
            self._thread_stop_request.set()
            self._scheduler_thread.join(timeout=10)
            self._scheduler_thread = None
            self._thread_stop_request = None

    def run_scheduler(self, async=True, flush_lost_jobs_interval=60):
        """Continuously poll for job status updates and schedule any jobs if
        there are open resources

        Args:
            async (bool, optional): whether to run the scheduler asynchronously
                in a thread
            flush_lost_jobs_interval (int, optional): how frequently to call
                the flush_lost_jobs() method. This method tends to be expensive
                so is only called intermittently
        """
        if async:
            self._thread_stop_request = Event()
            self._scheduler_thread = Thread(
                target=self._schedule, args=([flush_lost_jobs_interval]))
            self._scheduler_thread.start()
        else:
            self._schedule(flush_lost_jobs_interval=flush_lost_jobs_interval)


class SimpleScheduler(_BaseScheduler):
    """Continuously move jobs through the executor queue when jobs are ready
    to run.

    Args:
        executor (obj): instance of LocalExecutor or SGEExecutor
    """

    def _schedule(self, flush_lost_jobs_interval):
        start = timer()
        while self.keep_alive:
            # check if we want to
            check_lost_jobs = (timer() - start) > flush_lost_jobs_interval
            if check_lost_jobs:
                start = timer()

            # update all queues
            self.executor.refresh_queues(flush_lost_jobs=check_lost_jobs)


class RetryScheduler(_BaseScheduler):
    """Continuously move jobs through the executor queue when jobs are ready
    to run. Reschedule failed jobs.

    Args:
        executor (obj): instance of LocalExecutor or SGEExecutor
        retries (int, optional): how many times to reschedule failed jobs
    """

    def __init__(self, retries=1, *args, **kwargs):
        super(RetryScheduler, self).__init__(*args, **kwargs)

        self.retries = retries
        self._retry_limit_exceeded = []

    def _schedule(self, flush_lost_jobs_interval):
        start = timer()
        while self.keep_alive:

            # check for failures and reset their status
            reschedules = (
                set(self.executor.failed_jobs) -
                set(self._retry_limit_exceeded))
            for jid in reschedules:
                tries = len(self.executor.jobs[jid]["job"].job_instance_ids)

                if tries <= self.retries:
                    self.logger.info("job {} failed. Trying again. Try=#{}"
                                     .format(jid, tries + 1))
                    self.executor.jobs[jid]["status_id"] = None
                    self.executor.jobs[jid]["current_job_instance_id"] = None
                else:
                    self._retry_limit_exceeded.append(jid)

            # check if we want to
            check_lost_jobs = (timer() - start) > flush_lost_jobs_interval
            if check_lost_jobs:
                start = timer()

            # update all queues
            self.executor.refresh_queues(flush_lost_jobs=check_lost_jobs)
