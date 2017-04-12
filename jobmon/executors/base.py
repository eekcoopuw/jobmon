import logging

from jobmon.models import Status
from jobmon.subscriber import Subscriber
from jobmon.publisher import PublisherTopics


class BaseExecutor(object):

    def __init__(self, mon_dir, request_retries=3, request_timeout=3000,
                 parallelism=None, subscribe_to_job_state=True):
        self.logger = logging.getLogger(__name__)

        self.parallelism = parallelism

        # track job state
        self.jobs = {}

        # environment for distributed applications
        self.mon_dir = mon_dir
        self.request_retries = request_retries
        self.request_timeout = request_timeout

        # subscribe for published updates about job state
        if subscribe_to_job_state:
            self.subscriber = Subscriber(self.mon_dir)
            self.subscriber.connect(PublisherTopics.JOB_STATE.value)
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
        return self._jids_with_status(status_id=Status.UNKNOWN)

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

    def queue_job(self, job, *args, **kwargs):
        """Add a job definition to the executor's queue.

        Args:
            job (jobmon.job.Job): instance of jobmon.job.Job object
        """
        self.jobs[job.jid] = {
            "job": job,
            "args": args,
            "kwargs": kwargs,
            "status_id": None}

    def _poll_status(self):
        """poll for status updates that have been published by the central
           job monitor"""
        update = self.subscriber.recieve_update()
        while update is not None:
            jid, job_meta = update.items()[0]
            job_status = int(job_meta["job_instance_status_id"])
            try:
                self.jobs[int(jid)]["status_id"] = job_status
            except KeyError:
                pass
            update = self.subscriber.recieve_update()

    def refresh_queues(self):
        self._poll_status()
        self._flush_unknown()

        current_queue_length = len(self.queued_jobs)
        running_queue_length = len(self.running_jobs)
        self.logger.debug(
            "{} running job instances".format(running_queue_length))
        self.logger.debug("{} in queue".format(current_queue_length))

        # figure out how many jobs we can submit
        if not self.parallelism:
            open_slots = current_queue_length
        else:
            open_slots = self.parallelism - running_queue_length

        # submit the amount of jobs that our parallelism allows for
        for _ in range(min((open_slots, current_queue_length))):

            if self.queued_jobs:
                job_def = self.jobs[self.queued_jobs[0]]
                job = job_def["job"]
                job_instance_id = self.execute_async(
                    job,
                    *job_def["args"],
                    **job_def["kwargs"])

                # add reference to job class and the executor
                job.job_instance_ids.append(job_instance_id)
                self.jobs[job.jid] = {"job": job,
                                      "args": job_def["args"],
                                      "kwargs": job_def["kwargs"],
                                      "status_id": Status.SUBMITTED}
