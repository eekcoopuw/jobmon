import logging


class BaseExecutor(object):

    def __init__(self, mon_dir, request_retries=3, request_timeout=3000,
                 parallelism=None):
        self.logger = logging.getLogger(__name__)

        self.parallelism = parallelism

        # track job state
        self.queued_jobs = []  # order dependent execution queue
        self.running_jobs = []  # order independent execution tracker

        # environment for distributed applications
        self.mon_dir = mon_dir
        self.request_retries = request_retries
        self.request_timeout = request_timeout

        # execute start method
        self.start()

    def start(self):
        pass

    def stop(self):
        pass

    def queue_job(self, job, *args, **kwargs):
        """Add a job definition to the executors queue.

        Args:
            job (jobmon.job.Job): instance of jobmon.job.Job object
        """
        self.queued_jobs.append({
            "job": job,
            "args": args,
            "kwargs": kwargs})

    def refresh_queues(self):
        # Calling child class sync method
        self.logger.debug("Calling the {} sync method".format(self.__class__))
        self.sync()

        current_queue_length = len(list(self.queued_jobs))
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

            job_def = self.queued_jobs.pop(0)
            job = job_def.pop("job")

            # ideally execute async would return a job instance but we want to
            # reduce strain on the central job monitor so we wont construct the
            # job instance class
            job_instance_id = self.execute_async(
                job,
                *job_def["args"],
                **job_def["kwargs"])

            # add reference to job class and the executor
            job.job_instance_ids.append(job_instance_id)
            self.running_jobs.append(job_instance_id)
