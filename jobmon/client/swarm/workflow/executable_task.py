import logging
import hashlib
from typing import Optional, List, Dict

from jobmon.models.attributes.constants import job_attribute
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.job_management.swarm_job import SwarmJob

logger = logging.getLogger(__name__)


class ExecutableTask(object):
    """
    The root of the Task class tree.
    All tasks have a set of upstream and a set of downstream tasks.

    Executable jobs (in release Dugong) have a jobmon.Job, which is executed on
    the SGE cluster.
    External Tasks (fin release Frog) do not have Jobs, because they represent
    input tasks that are "givens" and cannot be executed.

    Do not subclass!
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

    def __init__(self, command: str, upstream_tasks: List = None,
                 env_variables: Dict = None, name: str = None, slots: int = None,
                 mem_free: int = None, num_cores: int = None,
                 max_runtime_seconds: int = None, queue: str = None,
                 max_attempts: int = 3, j_resource: bool = False,
                 tag: str = None, context_args: dict = None,
                 job_attributes: dict = None, m_mem_free: str = None,
                 executor_class: str = 'SGEExecutor',
                 executor_parameters: Optional[ExecutorParameters] = None):
        """
        Create a task

        Args:
            command: the unique command for this Task, also readable by humans. Should include all parameters. Two Tasks are equal (__eq__) iff they have the same command
            upstream_tasks: Task objects that must be run prior to this
            env_variables: any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
            name: name that will be visible in qstat for this job
            slots: slots to request on the cluster. Default is 1
            num_cores: number of cores to request on the cluster
            mem_free: amount of memory to request on the cluster.
                Generally 2x slots. Default is 1
            m_mem_free: amount of memory in gbs, tbs, or mbs, G, T, or M,
                to request on the fair cluster. Mutually exclusive with
                mem_free as it will fully replace that argument when the dev
                and prod clusters are taken offline
            max_attempts: number of attempts to allow the cluster to try
                before giving up. Default is 3
            max_runtime_seconds: how long the job should be allowed to
                run before the executor kills it. Default is None, for indefinite.
            tag: a group identifier. Currently just used for visualization.
                All tasks with the same tag will be colored the same in a
                TaskDagViz instance. Default is None.
            queue: queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            job_attributes: any attributes that will be
                tracked. Once the task becomes a job and receives a job_id,
                these attributes will be used for the job_factory
                add_job_attribute function
            j_resource: whether this task is using the j-drive or not
            context_args: additional args to be passed to the executor
            executor_class: the type of executor so we can instantiate the
                executor parameters properly
            executor_parameters: an instance of executor
                paremeters class
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

        self.max_attempts = max_attempts

        # Names of jobs can't start with a numeric.
        if name is None:
            self.name = "task_{}".format(self.hash)
        else:
            self.name = name
        self.hash_name = self.name  # for backwards compatibility
        self.tag = tag

        ExecutableTask.is_valid_job_name(self.name)

        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()
        self.downstream_tasks: set = set()
        for up in self.upstream_tasks:
            up.add_downstream(self)

        self.job_attributes = job_attributes

        if executor_parameters is None:
            self.executor_parameters = ExecutorParameters(
                slots=slots,
                num_cores=num_cores,
                mem_free=mem_free,
                m_mem_free=m_mem_free,
                max_runtime_seconds=max_runtime_seconds,
                queue=queue,
                j_resource=j_resource,
                context_args=context_args,
                executor_class=executor_class)
        else:
            self.executor_parameters = executor_parameters
        is_valid, msg = self.executor_parameters.is_valid()
        if not is_valid:
            logger.warning(msg)

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

    def add_job_attribute(self, attribute_type, value):
        """
        Add an attribute and value (key, value pair) to track in the task,
        throw an error if the attribute or value isn't the right type or
        if it is for usage data, which is not configured on the user side
        """
        user_cant_config = [job_attribute.WALLCLOCK, job_attribute.CPU,
                            job_attribute.IO, job_attribute.MAXRSS]
        if attribute_type in user_cant_config:
            raise ValueError(
                "Invalid attribute configuration for {} with name: {}, "
                "user input not used to configure attribute value".format(
                    attribute_type, type(attribute_type).__name__))
        elif not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif (not attribute_type == job_attribute.TAG and not int(value))\
                or (attribute_type == job_attribute.TAG and
                    not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))

        else:
            self.job_attributes[attribute_type] = value

    def add_job_attributes(self, dict_of_attributes):
        for attribute_type in dict_of_attributes:
            self.job_attributes[attribute_type] = dict_of_attributes[
                attribute_type]

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

    def __init__(self, task, job: SwarmJob, job_list_manager):
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
        self._task = task

        if task:
            self.hash = task.hash
        else:
            self.hash = None

    @property
    def is_bound(self):
        return (self._task is not None) and (self.hash is not None)

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
