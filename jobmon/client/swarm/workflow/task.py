from functools import partial
import hashlib

from typing import Optional, List, Callable, Union

from jobmon.models.attributes.constants import job_attribute
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class Task:

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @classmethod
    def is_valid_job_name(cls, name):
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
        elif any(e in name for e in cls.ILLEGAL_SPECIAL_CHARACTERS):
            raise ValueError("name contains illegal special character, "
                             "illegal characters are: '{}'"
                             .format(cls.ILLEGAL_SPECIAL_CHARACTERS)
                             )

        return True

    def __init__(self,
                 command: str,
                 node_arg_vals: dict,
                 data_arg_vals: dict,
                 executor_parameters: Union[ExecutorParameters, Callable],
                 name: Optional[str] = None,
                 upstream_tasks: Optional[List["ExecutableTask"]] = None,
                 max_attempts: Optional[int] = 3,
                 job_attributes: Optional[dict] = None):
        """
        Create a task

        Args:
            command: the unique command for this Task, also readable by humans
                Should include all parameters. Two Tasks are equal (__eq__)
                iff they have the same command
            upstream_tasks: Task objects that must be run prior to this
            name: name that will be visible in qstat for this job
            num_cores: number of cores to request on the cluster
            m_mem_free: amount of memory in gbs, tbs, or mbs, G, T, or M,
                to request on the fair cluster.
            max_attempts: number of attempts to allow the cluster to try
                before giving up. Default is 3
            max_runtime_seconds: how long the job should be allowed to run
                before the executor kills it. Default is None, for indefinite.
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
            resource_scales: for each resource, a scaling value (between 0 and 1)
                can be provided so that different resources get scaled differently.
                Default is {'m_mem_free': 0.5, 'max_runtime_seconds': 0.5},
                only resources that are provided will ever get adjusted
            hard_limits: if the user wants jobs to stay on the chosen queue
                and not expand if resources are exceeded, set this to true
            executor_class: the type of executor so we can instantiate the
                executor parameters properly
            executor_parameters: an instance of executor
                paremeters class

        Raise:
           ValueError: If the hashed command is not allowed as an SGE job name;
           see is_valid_job_name

        """
        self.command = command

        # Hash must be an integer, in order for it to be returned by __hash__
        self.hash = int(hashlib.sha1(command.encode('utf-8')).hexdigest(), 16)

        self.max_attempts = max_attempts

        # Names of jobs can't start with a numeric.
        if name is None:
            self.name = "task_{}".format(self.hash)
        else:
            self.name = name

        self.is_valid_job_name(self.name)

        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()
        self.downstream_tasks: set = set()
        for up in self.upstream_tasks:
            up.add_downstream(self)

        if job_attributes:
            self.job_attributes = job_attributes
        else:
            self.job_attributes = {}

        if isinstance(executor_parameters, ExecutorParameters):
            # if the resources have already been defined, function returns
            # itself upon evalutaion
            is_valid, msg = executor_parameters.is_valid()
            if not is_valid:
                logger.warning(msg)
            static_func = (
                lambda executor_parameters, *args: executor_parameters)
            self.executor_parameters = partial(static_func,
                                               executor_parameters)
        else:
            # if a callable was provided instead
            self.executor_parameters = partial(executor_parameters, self)
