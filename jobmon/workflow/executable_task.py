import logging
import hashlib

from jobmon.models import JobStatus

logger = logging.getLogger(__name__)


class ExecutableTask(object):
    """
    A Task is the _intent_ to run something.
    A Job is the schedulable thing (and JobInstance etc).
    This is an abstract class, actual applications will subclass this with
    specific Tasks (eg 'calculate percentage change)'
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @staticmethod
    def is_valid_sge_job_name(name):
        """
        If the name is invalid it will raises an exception. The list of illegal
        characters might not be complete, I could not find an official list.

        TBD This should probably be moved to the cluster_utils package

        Must:
          - Not be null or the empty string
          - being with a digit
          - contain am illegal character

        Args:
            name:

        Returns:
            True (or raises)

        Raises:
            ValueError: if the name is not valid.
        """

        if not name:
            raise ValueError("name cannot be None or empty")
        elif name[0].isdigit():
            raise ValueError("name cannot begin with a digit, saw: '{}'"
                             .format(name[0]))
        elif any(e in name for e in ExecutableTask.ILLEGAL_SPECIAL_CHARACTERS):
            raise ValueError("name contains illegal special character, "
                             "illegal characters are: '{}'"
                             .format(ExecutableTask.ILLEGAL_SPECIAL_CHARACTERS)
                             )

        return True

    def __init__(self, command, upstream_tasks=None):
        """
        Create a task

        Args
         command: the unique command for this Task, also readable by humans.
         Should include all parameters.
         Two Tasks are equal (__eq__) iff they have the same command

         Raise:
           ValueError: If the hashed command is not allowed as an SGE job name;
           see is_valid_sge_job_name
        """
        # Hash must be an integer, in order for it to be returned by __hash__
        self.hash = int(hashlib.sha1(command.encode('utf-8')).hexdigest(), 16)
        # Names of sge jobs can't start with a numeric.
        self.hash_name = "task_{}".format(self.hash)
        ExecutableTask.is_valid_sge_job_name(self.hash_name)

        self.job_id = None  # will be None until executed
        # self.job = None  # cached, could be None in resume use case until
        # Job resurrected from dbs
        self.cached_status = JobStatus.INSTANTIATED

        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()
        self.downstream_tasks = set()
        for up in self.upstream_tasks:
            up.add_downstream(self)

    def __eq__(self, other):
        """
        Two tasks are equal if they have the same hash_name.
        Needed for sets
        """
        return self.hash == other.hash

    def __hash__(self):
        """
        Logic must match __eq__
        """
        return self.hash

    def get_status(self):
        """
        For executable jobs, my status is the status of my Job
        Cached because the status is returned from the
        block_until_any_done_or_error calls to job_list_manager, rather than by
        retrieving the entire job from the database (for efficiency).

        Returns:
            JobStatus
        """
        return self.cached_status

    def set_status(self, new_status):
        self.cached_status = new_status

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

    def add_upstream(self, ancestor):
        """
        Add an upstream (ancestor) Task. This has Set semantics, an upstream
        task will only be added once. Symmetrically, this method also adds this
        Task as a downstream on the ancestor.
        """
        self.upstream_tasks.add(ancestor)
        # avoid endless recursion, set directly
        ancestor.downstream_tasks.add(self)

    def add_downstream(self, descendent):
        """
        Add an downstream (ancestor) Task. This has Set semantics, a downstream
        task will only be added once. Symmetrically, this method also adds this
        Task as an upstream on the ancestor.
        """
        self.downstream_tasks.add(descendent)
        # avoid endless recursion, set directly
        descendent.upstream_tasks.add(self)

    def create_job(self, job_list_manager):
        """
        Abstract, must be overridden.
        This MUST set self.job_id

        Args:
            job_list_manager: Used to create the Job

        Returns:
            The job_id of the new Job
        """
        raise NotImplementedError()

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
                   status=self.cached_status)
