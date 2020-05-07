from __future__ import annotations

from functools import partial
import hashlib
from http import HTTPStatus as StatusCodes
from typing import Optional, List, Callable, Union, Tuple

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.node import Node
from jobmon.client.requests.requester import Requester
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.exceptions import InvalidResponse
from jobmon.models.task_status import TaskStatus


logger = logging.getLogger(__name__)


class Task:

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @classmethod
    def is_valid_job_name(cls, name: str) -> bool:
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
                 max_attempts: int = 3,
                 upstream_tasks: List[Task] = [],
                 task_attributes: Union[List, dict] = {},
                 requester: Requester = shared_requester):
        """
        Create a task

        Args:
            command (str): the unique command for this Task, also readable by
                humans. Should include all parameters. Two Tasks are equal
                (__eq__) iff they have the same command
            task_template_version_id (int): identifer for the associated
                Task Template
            node_args (dict): Task arguments that identify a unique node
                in the DAG
            task_args (dict): Task arguments that make the command unique
                across workflows usually pertaining to data flowing through
                the task
            executor_parameters: callable to be evaluated and assign
                resources or static instance of ExecutorParameters class
            name (str): name that will be visible in qstat for this job
            upstream_tasks (List): Task objects that must be run prior to this
            max_attempts (int): number of attempts to allow the cluster to try
                before giving up. Default is 3
            task_attributes (list or dict): dictionary of attributes and their
                values or list of attributes that will be assigned later
            requester (Requester): requester to communicate with the flask
                services

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

        # upstream and downstream task relationships
        self.upstream_tasks = set(upstream_tasks) if upstream_tasks else set()
        self.downstream_tasks: set = set()

        for task in upstream_tasks:
            self.add_upstream(task)

        self.task_attributes: dict = {}
        if isinstance(task_attributes, List):
            for attr in task_attributes:
                self.task_attributes[attr] = None
        elif isinstance(task_attributes, dict):
            for attr in task_attributes:
                self.task_attributes[str(attr)] = str(task_attributes[attr])
        else:
            raise ValueError("task_attributes must be provided as a list of "
                             "attributes or a dictionary of attributes and "
                             "their values")

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

    @property
    def task_id(self) -> int:
        if not hasattr(self, "_task_id"):
            raise AttributeError(
                "task_id cannot be accessed before task is bound")
        return self._task_id

    @property
    def initial_status(self) -> str:
        # TODO: remove status from this object
        if not hasattr(self, "_initial_status"):
            raise AttributeError("initial_status cannot be accessed before task is "
                                 "bound")
        return self._initial_status

    @property
    def workflow_id(self) -> int:
        if not hasattr(self, "_workflow_id"):
            raise AttributeError(
                "workflow_id cannot be accessed via task before a workflow is "
                "bound")
        return self._workflow_id

    @workflow_id.setter
    def workflow_id(self, val) -> int:
        self._workflow_id = val

    def bind(self, reset_if_running: bool = True) -> int:
        task_id, status = self._get_task_id_and_status()
        if task_id is None:
            task_id = self._add_task()
            status = TaskStatus.REGISTERED
        else:
            status = self._update_task_parameters(task_id, reset_if_running)
        self._task_id = task_id
        self._initial_status = status
        return task_id

    def add_upstream(self, ancestor: Task) -> None:
        """
        Add an upstream (ancestor) Task. This has Set semantics, an upstream
        task will only be added once. Symmetrically, this method also adds this
        Task as a downstream on the ancestor.
        """
        self.upstream_tasks.add(ancestor)
        ancestor.downstream_tasks.add(self)

        self.node.add_upstream_node(ancestor.node)

    def add_downstream(self, descendent: Task) -> None:
        """
        Add an downstream (ancestor) Task. This has Set semantics, a downstream
        task will only be added once. Symmetrically, this method also adds this
        Task as an upstream on the ancestor.
        """
        self.downstream_tasks.add(descendent)
        descendent.upstream_tasks.add(self)

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

    def _get_task_id_and_status(self) -> Tuple[Optional[int], Optional[str]]:
        app_route = '/task'
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
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')
        return response['task_id'], response['task_status']

    def _update_task_parameters(self, task_id: int, reset_if_running: bool
                                ) -> str:
        app_route = f'/task/{task_id}/update_parameters'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                'name': self.name,
                'command': self.command,
                'max_attempts': self.max_attempts,
                'reset_if_running': reset_if_running,
                'task_attributes': self.task_attributes
            },
            request_type='put'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from PUT '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')
        return response["task_status"]

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
                'task_args': self.task_args,
                'task_attributes': self.task_attributes
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')
        # TODO: figure out why Megan wanted to return
        # response["task_attribute_ids"]
        return response["task_id"]

    def add_attributes(self, task_attributes: dict) -> None:
        """Function that users can call either to update values of existing
        attributes or add new attributes"""
        app_route = f'/task/{self.task_id}/task_attributes'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"task_attributes": task_attributes},
            request_type="put"
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexepected status code {return_code} from PUT '
                             f'request through route {app_route}. Expected '
                             f'code 200. Response content: {response}')

    def add_attribute(self, attribute: str, value: str) -> str:
        """Function that users can call to add a single attribute for a
        task"""
        self.task_attributes[str(attribute)] = str(value)
        # if the task has already been bound, bind the attributes
        if self._task_id:
            self.add_attributes({str(attribute): str(value)})

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
