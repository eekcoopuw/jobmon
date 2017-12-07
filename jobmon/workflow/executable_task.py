import logging
import getpass

from jobmon.models import JobStatus
from jobmon.workflow.abstract_task import AbstractTask

logger = logging.getLogger(__name__)


class ExecutableTask(AbstractTask):
    """
    A Task is the _intent_ to run something.
    A Job is the schedulable thing (and JobInstance etc).
    This is an abstract class, actual applications will subclass this with
    specific Tasks (eg 'calculate percentage change)'
    """

    def __init__(self, command, upstream_tasks=None):
        AbstractTask.__init__(self, command)
        self.job_id = None  # will be None until executed
        # self.job = None  # cached, could be None in resume use case until
        # Job resurrected from dbs
        self.status = None  # will be None until bound to DB

        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()

        for up in self.upstream_tasks:
            up.add_downstream(self)

    def get_status(self):
        """
        For executable jobs, my status is the status of my Job
        Cached because the status is returned from the
        block_until_any_done_or_error calls to job_list_manager, rather than by
        retrieving the entire job from the database (for efficiency).

        Returns:
            JobStatus
        """
        return self.status

    def set_status(self, new_status):
        self.status = new_status

    def is_done(self):
        """
        If my Job is not "DONE" then I must be run.

        Only called when all of the upstream are DONE - either they completed
        successfully or they were skipped because were not out of date, or this
        is top_fringe (has no upstreams). Failed upstreams will NOT cause this
        method to be called.

        Delegates to Job

        DOES NOT NEED TO BE OVERRIDDEN
        """
        if not self.get_status() == JobStatus.DONE:
            logger.debug("am I done? {}; No (not DONE)".format(self))
            return False
        else:
            logger.debug("am I done? {}; Yes (already DONE)"
                         .format(self))
            return True

    def all_upstreams_done(self):
        """
        Are all my upstreams marked done in this execution cycle?

        DOES NOT NEED TO BE OVERRIDDEN
        Returns:
            True if no upstreams, or they are all DONE.
        """
        logger.debug("Checking all upstreams for {}".format(self))
        for task in self.upstream_tasks:
            logger.debug("  Examine {}".format(task))
            if not task.get_status() == JobStatus.DONE:
                return False
        return True

    def bind(self, job_list_manager):
        """
        Abstract, must be overridden.
        This MUST set self.job_id

        Args:
            job_list_manager: Used to create the Job

        Returns:
            The job_id of the new Job
        """
        logger.debug("Create job, command = {}".format(self.command))

        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.command,
            slots=1,
            mem_free=2,
            max_attempts=3,
            stderr=('/ihme/scratch/users/{}/stderr/stderr-$JOB_ID-{}.txt'
                    .format(getpass.getuser(), self.hash_name)),
            stdout=('/ihme/scratch/users/{}//stdout/stdout-$JOB_ID-{}.txt'
                    .format(getpass.getuser(), self.hash_name))
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @property
    def is_bound(self):
        """Boolean indicating whether the Task is bound to the DB"""
        if self.job_id:
            return True
        else:
            return False

    def queue_job(self, job_list_manager):
        """
        Ask the job_list_manager to queue the job.
         DOES NOT NEED TO BE OVERRIDDEN

        Args:
             job_list_manager:

        Returns:
            The job_id (for convenience)

        Raises:
            ValueError if my job_id is None
        """
        if not self.job_id:
            raise ValueError("Cannot queue Task because job_id is None: {}"
                             .format(self))
        job_list_manager.queue_job(self.job_id)
        return self.job_id

    def __repr__(self):
        """
        Very useful for logging-based debugging
        Returns:
             String with information useful for a log message
        """
        return "[Task: jid={jid}, '{name}', status: {status}]". \
            format(jid=self.job_id, name=self.hash_name,
                   status=self.status)
