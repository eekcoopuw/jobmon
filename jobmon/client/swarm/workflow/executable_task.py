import logging
import hashlib

from jobmon.models.job_status import JobStatus

logger = logging.getLogger(__name__)


class ExecutableTask(object):
    """
    The root of the Task class tree.
    All tasks have a set of upstream and a set of downstream tasks.

    Executable jobs (in release Dugong) have a jobmon.Job, which is executed on
    the SGE cluster.
    External Tasks (fin release Frog) do not have Jobs, because they represent
    input tasks that are "givens" and cannot be executed.
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @staticmethod
    def is_valid_job_name(name):
        """
        If the name is invalid it will raises an exception. Primarily based on
        the restrictions SGE places on job names. The list of illegal
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

    def __init__(self, command, upstream_tasks=None, env_variables={},
                 name=None, slots=1, mem_free=2, max_attempts=3,
                 max_runtime=None, tag=None, context_args=None):
        """
        Create a task

        Args
         command: the unique command for this Task, also readable by humans.
            Should include all parameters.
            Two Tasks are equal (__eq__) iff they have the same command
        upstream_tasks (list): Task objects that must be run prior to this
        env_variables (dict): any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
        name (str): name that will be visible in qstat for this job
        slots (int): slots to request on the cluster. Default is 1
        mem_free (int): amount of memory to request on the cluster.
            Generally 2x slots. Default is 2
        max_attempts (int): number of attempts to allow the cluster to try
            before giving up. Default is 1
        max_runtime (int, seconds): how long the job should be allowed to
            run before the executor kills it. Default is None, for indefinite.
        tag (str): a group identifier. Currently just used for visualization.
            All tasks with the same tag will be colored the same in a
            TaskDagViz instance. Default is None.

         Raise:
           ValueError: If the hashed command is not allowed as an SGE job name;
           see is_valid_job_name
        """
        if env_variables:
            env_variables = ' '.join('{}={}'.format(key, val) for key, val
                                     in env_variables.items())
            command = ' '.join([env_variables, command])
        self.command = command

        # Hash must be an integer, in order for it to be returned by __hash__
        self.hash = int(hashlib.sha1(command.encode('utf-8')).hexdigest(), 16)

        self.slots = slots
        self.mem_free = mem_free
        self.max_attempts = max_attempts
        self.max_runtime = max_runtime
        self.context_args = context_args

        # Names of jobs can't start with a numeric.
        if name is None:
            self.name = "task_{}".format(self.hash)
        else:
            self.name = name
        self.hash_name = self.name  # for backwards compatibility
        self.tag = tag

        ExecutableTask.is_valid_job_name(self.name)

        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()
        self.downstream_tasks = set()
        for up in self.upstream_tasks:
            up.add_downstream(self)

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

    def __eq__(self, other):
        """
        Two tasks are equal if they have the same hash.
        Needed for sets
        """
        return self.hash == other.hash

    def __hash__(self):
        """Logic must match __eq__"""
        return self.hash

    def __lt__(self, other):
        """Logic must match __eq__"""
        return self.hash < other.hash

    def __repr__(self):
        """
        Very useful for logging-based debugging
        Returns:
             String with information useful for a log message
        """
        return "[Task: hash={hs}, '{name}']". \
            format(hs=self.hash, name=self.name)


class BoundTask(object):
    """The class that bridges the gap between a task and it's bound Job"""

    def __init__(self, task, job, job_list_manager):
        """
        Link task and job

        Args
            task (obj): obj of a class inherited from ExecutableTask
            job (obj): obj of type models.Job
            job_list_manager (obj): obj of type JobListManager
        """
        self.job_id = job.job_id
        self.status = job.status

        self._jlm = job_list_manager
        self._job = job
        self._task = task

        if task:
            self.hash = task.hash
        else:
            self.hash = None

    @property
    def all_upstreams_done(self):
        """Return a bool of if upstreams are done or not"""
        return all([u.is_done for u in self.upstream_tasks])

    @property
    def is_done(self):
        """Return a book of if this job is done or now"""
        return self.status == JobStatus.DONE

    @property
    def downstream_tasks(self):
        """Return list of downstream tasks"""
        return [self._jlm.bound_task_from_task(task)
                for task in self._task.downstream_tasks]

    @property
    def upstream_tasks(self):
        """Return a list of upstream tasks"""
        return [self._jlm.bound_task_from_task(task)
                for task in self._task.upstream_tasks]
