import logging

from jobmon.models import Status
from jobmon.subscriber import Subscriber
from jobmon.publisher import PublisherTopics


class BaseExecutor(object):

    def __init__(self, monitor_connection=None, publisher_connection=None, parallelism=None,
                 subscribe_to_job_state=True):
        """@TODO Document the two connections"""
        self.logger = logging.getLogger(__name__)
        self.monitor_connection = monitor_connection
        self.publisher_connection = publisher_connection
        self.parallelism = parallelism

        # track job state
        self.jobs = {}

        if subscribe_to_job_state and not monitor_connection:
            raise ValueError("monitor_connection is required if "
                             "subscribe_to_job_state=True")
        # environment for distributed applications
        self.monitor_connection = monitor_connection

        if subscribe_to_job_state and not publisher_connection:
            raise ValueError("publisher_connection is required if "
                             "subscribe_to_job_state=True")
            # environment for distributed applications
        self.publisher_connection = publisher_connection

        # subscribe for published updates about job state
        if subscribe_to_job_state:
            self.subscriber = Subscriber(self.publisher_connection)
            self.subscriber.connect(PublisherTopics.JOB_STATE)
        else:
            self.subscriber = None

        # execute start method
        self.start()

    @property
    def queued_jobs(self):
        return self._jids_with_status(status_id=None)

    @property
    def running_jobs(self):
        jids = []
        for status_id in [Status.SUBMITTED, Status.RUNNING]:
            jids.extend(self._jids_with_status(status_id=status_id))
        return jids

    @property
    def failed_jobs(self):
        return self._jids_with_status(status_id=Status.FAILED)

    @property
    def completed_jobs(self):
        return self._jids_with_status(status_id=Status.COMPLETE)

    @property
    def unknown_jobs(self):
        return self._jids_with_status(status_id=Status.UNREGISTERED_STATE)

    def _jids_with_status(self, status_id=None):
        jids = []
        for j in self.jobs.keys():
            if self.jobs[j]["status_id"] == status_id:
                jids.append(j)
        return jids

    def _jid_from_job_instance_id(self, job_instance_id):
        for j in self.jobs.keys():
            if job_instance_id in self.jobs[j]["job"].job_instance_ids:
                return j
        raise ValueError("No job_id associated with job_instance_id: {}"
                         "".format(job_instance_id))

    def start(self):
        pass

    def stop(self):
        pass

    def queue_job(self, job, process_timeout=None, *args, **kwargs):
        """Add a job definition to the executor's queue.

        Args:
            job (jobmon.job.Job): instance of jobmon.job.Job object
            process_timeout (int, optional): time in seconds to wait for
                process to finish. default is forever
        """
        self.jobs[job.jid] = {
            "job": job,
            "process_timeout": process_timeout,
            "args": args,
            "kwargs": kwargs,
            "status_id": None}

    def _poll_status(self):
        """poll for status updates that have been published by the central
           job monitor"""
        update = self.subscriber.receive_update()
        while update is not None:
            jid, job_meta = update.items()[0]
            job_status = int(job_meta["job_instance_status_id"])
            try:
                self.jobs[int(jid)]["status_id"] = job_status
            except KeyError:
                pass
            update = self.subscriber.receive_update()

    def refresh_queues(self, flush_lost_jobs=True):
        """update the queues to reflect the current state each job

        Args:
            flush_lost_jobs (bool, optional): whether to call flush_lost_jobs()
                method to clean up any jobs that died unexpectedly and
                didn't emit a status update to the central job monitor
        """
        self._poll_status()
        if flush_lost_jobs:
            self.logger.debug("consolidating any lost jobs")
            self.flush_lost_jobs()

        current_queue_length = len(self.queued_jobs)
        running_queue_length = len(self.running_jobs)

        # figure out how many jobs we can submit
        if not self.parallelism:
            open_slots = current_queue_length
        else:
            open_slots = self.parallelism - running_queue_length

        # submit the amount of jobs that our parallelism allows for
        for _ in range(min((open_slots, current_queue_length))):

            self.logger.debug(
                "{} running job instances".format(running_queue_length))
            self.logger.debug("{} in queue".format(current_queue_length))

            if self.queued_jobs:
                job_def = self.jobs[self.queued_jobs[0]]
                job = job_def["job"]
                job_instance_id = self.execute_async(
                    job,
                    process_timeout=job_def["process_timeout"],
                    *job_def["args"],
                    **job_def["kwargs"])

                # add reference to job class and the executor
                job.job_instance_ids.append(job_instance_id)
                self.jobs[job.jid]["job"] = job
                self.jobs[job.jid]["status_id"] = Status.SUBMITTED
