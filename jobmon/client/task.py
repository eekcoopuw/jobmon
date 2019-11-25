from __future__ import annotations

from functools import partial
import hashlib
from http import HTTPStatus as StatusCodes
from typing import Optional, List, Callable, Union

from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.node import Node
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm.executors.base import ExecutorParameters


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
            raise ValueError(
                f"name cannot begin with a digit, saw: '{name[0]}'")
        elif any(e in name for e in cls.ILLEGAL_SPECIAL_CHARACTERS):
            raise ValueError(
                "name contains illegal special character, illegal characters "
                f"are: '{cls.ILLEGAL_SPECIAL_CHARACTERS}'")
        return True

    def __init__(self,
                 command: str,
                 task_template_version_id: int,
                 node_args: dict,
                 task_args: dict,
                 executor_parameters: Union[ExecutorParameters, Callable],
                 name: Optional[str] = None,
                 max_attempts: Optional[int] = 3,
                 upstream_tasks: List[Task] = [],
                 job_attributes: Optional[dict] = None,
                 requester: Requester = shared_requester):
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
        self.requester = requester

        # pre bind hash defining attributes
        self.task_args = task_args
        self.task_args_hash = self._hash_task_args()
        self.node = Node(task_template_version_id, node_args, self)

        # pre bind mutable attributes
        self.command = command

        # Names of jobs can't start with a numeric.
        if name is None:
            self.name = f"task_{hash(self)}"
        else:
            self.name = name
        self.is_valid_job_name(self.name)

        # number of attempts
        self.max_attempts = max_attempts

        for task in upstream_tasks:
            self.add_upstream(task)

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

        if job_attributes:
            self.job_attributes = job_attributes
        else:
            self.job_attributes = {}

    @property
    def task_id(self) -> int:
        if not hasattr(self, "_task_id"):
            raise AttributeError(
                "task_id cannot be accessed before task is bound")
        return self._task_id

    @property
    def workflow_id(self) -> int:
        if not hasattr(self, "_workflow_id"):
            raise AttributeError(
                "workflow_id cannot be accessed via task before a workflow is "
                "bound")
        return self._workflow_id

    @workflow_id.setter
    def workflow_id(self, val):
        self._workflow_id = val

    def bind(self) -> int:
        task_id = self._get_task_id()
        if task_id is None:
            task_id = self._add_task()
        else:
            self._update_task_parameters()
        self._task_id = task_id
        return task_id

    def add_upstream(self, ancestor: Task):
        """
        Add an upstream (ancestor) Task. This has Set semantics, an upstream
        task will only be added once. Symmetrically, this method also adds this
        Task as a downstream on the ancestor.
        """
        self.node.add_upstream_node(ancestor.node)

    def add_downstream(self, descendent: Task):
        """
        Add an downstream (ancestor) Task. This has Set semantics, a downstream
        task will only be added once. Symmetrically, this method also adds this
        Task as an upstream on the ancestor.
        """
        self.node.add_downstream_node(descendent.node)

    def _hash_task_args(self) -> int:
        """a task_arg_hash is a hash of the encoded result of the args and
        values concatenated together"""
        arg_ids = list(self.task_args.keys())
        arg_ids.sort()

        arg_values = [str(self.task_args[key]) for key in arg_ids]
        arg_ids = [str(arg) for arg in arg_ids]

        hash_value = int(hashlib.sha1(''.join(arg_ids + arg_values).encode(
            'utf-8')).hexdigest(), 16)
        return hash_value

    def _get_task_id(self) -> Optional[int]:
        return_code, response = self.requester.send_request(
            app_route='/task',
            message={
                'workflow_id': self.workflow_id,
                'node_id': self.node.node_id,
                'task_args_hash': self.task_args_hash
            },
            request_type='get'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /task. Expected code 200.'
                             f' Response content: {response}')
        return response['task_id']

    def _update_task_parameters(self):
        app_route = f'/task/{self.task_id}/update_parameters'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                'name': self.name,
                'command': self.command,
                'max_attempts': self.max_attempts
            },
            request_type='put'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from PUT '
                             f'request through route {app_route}. Expected '
                             f'code 200. Response content: {response}')

    def _add_task(self) -> int:
        app_route = f'/task'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                'workflow_id': self.workflow_id,
                'node_id': self.node.node_id,
                'task_args_hash': self.task_args_hash,
                'name': self.name,
                'command': self.command,
                'max_attempts': self.max_attempts,
                'task_args': self.task_args
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from PUT '
                             f'request through route {app_route}. Expected '
                             f'code 200. Response content: {response}')
        return response["task_id"]

    def __eq__(self, other: Task) -> bool:
        return hash(self) == hash(other)

    def __lt__(self, other: Task) -> bool:
        return hash(self) < hash(other)

    def __hash__(self) -> int:
        hash_value = hashlib.sha1()
        hash_value.update(
            bytes(str(hash(self.node)).encode('utf-8')))
        hash_value.update(
            bytes(str(self.task_args_hash).encode('utf-8')))
        return int(hash_value.hexdigest(), 16)
