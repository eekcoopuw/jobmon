import logging
import time

from jobmon import requester
from jobmon.job import Job
from jobmon.exceptions import (CannotConnectToCentralJobMonitor,
                               CentralJobMonitorNotAlive)


class MonitoredQ(object):
    """monitored Q supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    def __init__(self, executor, max_alive_wait_time=45):
        """
        Args:
            mon_dir (string): directory where monitor server is running
            max_alive_wait_time (int, optional): how long to wait for an alive
                signal from the central job monitor
            request_retries (int, optional): how many times to attempt to
                communicate with the central job monitor per request
            request_timeout (int, optional): how long till each communication
                attempt times out
        """
        self.logger = logging.getLogger(__name__)

        self.jobs = {}
        self.executor = executor

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
            runfile (sting): full path to python executable file.
            jobname (sting): what name to register the sge job under.
            parameters (list, optional): command line arguments to be passed
                into runfile.

        """
        job = Job(self.executor.mon_dir, name=jobname, runfile=runfile,
                  job_args=parameters)
        self.jobs[job.jid] = job
        return job

    def queue_job(
            self, job, *args, **kwargs):
        """queue a job in the executor

        Args:
            job (jobmon.job.Job): instance of jobmon job.

            args and kwargs are passed through to the executors exec_async
            method
        """
        self.executor.queue_job(job, *args, **kwargs)

    def check_pulse(self, poll_interval=60):
        """continuously run executors heartbeat method until all queued jobs
        have finished

        Args:
            poll_interval (int, optional): how frequently to call the heartbeat
                method which updates the queue.
        """
        sgexec = self.executor
        sgexec.heartbeat()
        while len(sgexec.queued_jobs) > 0 or len(sgexec.running_jobs) > 0:
            time.sleep(poll_interval)
            sgexec.heartbeat()
