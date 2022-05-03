from __future__ import annotations

from collections.abc import Iterable
from http import HTTPStatus as StatusCodes
from itertools import product
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, TYPE_CHECKING, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.node import Node
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

logger = logging.getLogger(__name__)


class Array:
    """Representation of a client array object.

    Supports functionality to create tasks from the cross product of provided node_args.
    """

    def __init__(
        self,
        task_template_version: TaskTemplateVersion,
        task_args: Dict[str, Any],
        op_args: Dict[str, Any],
        cluster_name: str,
        max_concurrently_running: int = 10_000,
        max_attempts: int = 3,
        upstream_tasks: Optional[List[Task]] = None,
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialize the array object."""
        # task template attributes
        self.task_template_version = task_template_version

        # array name
        if name:
            self._name = name
        else:
            self._name = (
                task_template_version.task_template.template_name
                + '_'.join('{}={}'.format(*p) for p in task_args.items())
            )
            self._name = self._name if len(self._name) < 255 else self._name[0:254]

        # array attributes
        self.max_concurrently_running = max_concurrently_running

        # task passthrough attributes
        self.task_args = task_args
        self.op_args = op_args

        # global upstreams
        if upstream_tasks is None:
            upstream_tasks = []
        self.upstream_tasks = upstream_tasks

        # compute resources
        if not cluster_name:
            cluster_name = self.task_template_version.default_cluster_name
        self._instance_cluster_name = cluster_name

        self._instance_compute_resource = (
            compute_resources if compute_resources is not None else {}
        )
        self.compute_resources_callable = compute_resources_callable
        self.resource_scales = resource_scales
        self.max_attempts = max_attempts
        self._task_resources: Optional[TaskResources] = None  # Initialize to None

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.tasks: Dict[int, Task] = {}  # Initialize to empty dict

    @property
    def is_bound(self) -> bool:
        """If the array has been bound to the db."""
        return hasattr(self, "_array_id")

    @property
    def array_id(self) -> int:
        """If the array is bound then it will have been given an id."""
        if not self.is_bound:
            raise AttributeError("array_id cannot be accessed before workflow is bound")
        return self._array_id

    @array_id.setter
    def array_id(self, val: int) -> None:
        """Set the array id."""
        self._array_id = val

    @property
    def name(self) -> str:
        """Return the array name."""
        return self._name

    @property
    def task_resources(self) -> TaskResources:
        """Return the array's task resources."""
        if self._task_resources is not None:
            return self._task_resources
        else:
            raise AttributeError(
                "Task resources cannot be accessed before it is assigned"
            )

    @task_resources.setter
    def task_resources(self, task_resources: TaskResources) -> None:
        """Set the task resources.

        The task resources object must be bound to be set.
        """
        if not task_resources.is_bound:
            raise AttributeError(
                "Task resource must be bound and have an ID to be assigned to the array."
            )
        self._task_resources = task_resources

    @property
    def compute_resources(self) -> Dict[str, Any]:
        """A dictionary that includes the users requested resources for the current run.

        E.g. {cores: 1, mem: 1, runtime: 60, queue: all.q}"""
        try:
            resources = self.workflow.default_compute_resources_set.get(
                self.cluster_name, {}
            ).copy()
        except AttributeError:
            resources = {}
        resources.update(
            self.task_template_version.default_compute_resources_set.get(
                self.cluster_name, {}
            ).copy()
        )
        resources.update(self._instance_compute_resource.copy())
        return resources

    @property
    def cluster_name(self) -> str:
        """The name of the cluster the user wants to run their task on."""
        cluster_name = self._instance_cluster_name
        if not cluster_name:
            try:
                cluster_name = self.workflow.default_cluster_name
            except AttributeError:
                raise ValueError(
                    "cluster_name must be specified on workflow, task_template, or array"
                )
        return cluster_name

    @property
    def workflow(self) -> Workflow:
        """Get the workflow id if it has been bound to the db."""
        if not hasattr(self, "_workflow"):
            raise AttributeError(
                "workflow cannot be accessed via task before workflow is added to workflow"
            )
        return self._workflow

    @workflow.setter
    def workflow(self, val: Workflow) -> None:
        """Set the workflow id."""
        self._workflow = val

    def add_task(self, task: Task):
        """Add a task to an array.

        Set semantics - add tasks once only, based on hash name.

        Args:
            task: single task to add.
        """
        if task.cluster_name and self.cluster_name != task.cluster_name:
            raise ValueError(
                "Task assigned to different cluster than associated array. Task.cluster_name="
                f"{task.cluster_name}. Array.cluster_name={self.cluster_name}"
            )

        task_hash = hash(task)
        if task_hash in self.tasks.keys():
            raise ValueError(
                f"A task with hash {task_hash} already exists. All tasks in an Array must have"
                f" unique commands. Your command was: {task.command}"
            )

        self.tasks[task_hash] = task

        # populate backref
        task.array = self

    def create_tasks(
        self,
        upstream_tasks: Optional[List[Task]] = None,
        task_attributes: Union[List, dict] = {},
        max_attempts: int = 3,
        resource_scales: Optional[Dict[str, Any]] = None,
        **node_kwargs: Any,
    ) -> List[Task]:
        """Create an task associated with the array.

        Args:
            upstream_tasks: Task objects that must be run prior to this one
            task_attributes (dict or list): attributes and their values or just the attributes
                that will be given values later
            max_attempts: Number of attempts to try this task before giving up. Default is 3.
            resource_scales: determines the scaling factor for how aggressive resource
                adjustments will be scaled up
            **node_kwargs: values for each node argument specified in command_template

        Raises:
            ValueError: if the args that are supplied do not match the args in the command
                template.
        """
        if upstream_tasks is None:
            # If not specified, defined from the array upstreams
            upstream_tasks = self.upstream_tasks

        # Expand the node_args
        if not set(node_kwargs.keys()).issuperset(self.task_template_version.node_args):
            raise ValueError(
                f"Missing node_args for this array. Task Template requires node_args="
                f"{self.task_template_version.node_args}, got {set(node_kwargs.keys())}"
            )
        node_args_expanded = Array.expand_dict(**node_kwargs)

        # build tasks over node_args
        tasks = []
        for node_args in node_args_expanded:

            # build node
            node = Node(self.task_template_version, node_args, self.requester)

            task = Task(
                node=node,
                task_args=self.task_args,
                op_args=self.op_args,
                resource_scales=resource_scales,
                max_attempts=max_attempts,
                upstream_tasks=upstream_tasks,
                task_attributes=task_attributes,
                requester=self.requester,
            )
            tasks.append(task)

            logger.debug(f"Adding Task {task}")
            if hash(task) in self.tasks.keys():
                raise ValueError(
                    f"A task with hash {hash(task)} already exists. "
                    f"All tasks in a workflow must have unique "
                    f"commands. Your command was: {task.command}"
                )
            self.add_task(task)

        return tasks

    @staticmethod
    def expand_dict(**kwargs: Any) -> Iterator[Dict]:
        """Expand a dictionary of iterables into combinations of values.

        Given kwargs and values corresponding to node_args,
        return a dict of combinations of those node_args. Values of kwargs must be iterables.
        """
        # Return empty iterator if no arguments provided
        if len(kwargs) == 0:
            return iter(())
        keys = kwargs.keys()
        for element in product(*kwargs.values()):
            yield dict(zip(keys, element))

    def get_tasks_by_node_args(self, **kwargs: Any) -> List["Task"]:
        """Query tasks by node args. Used for setting dependencies."""
        tasks: List["Task"] = []

        for task in self.tasks.values():
            key_count_to_meet = len(kwargs)
            for key, val in kwargs.items():
                if (
                    isinstance(val, Iterable)
                    and task.node.node_args[key] in val
                    or task.node.node_args[key] == val
                ):
                    key_count_to_meet -= 1
                    continue
                else:
                    break
            if key_count_to_meet == 0:
                tasks.append(task)

        return tasks

    def validate(self):
        # check
        dependent_tasks = set()
        for task in self.tasks.values():
            dependent_tasks.union(task.upstream_tasks)
            dependent_tasks.union(task.downstream_tasks)
        cyclical_tasks = dependent_tasks.intersection(set(self.tasks.values()))
        if cyclical_tasks:
            raise ValueError(
                f"A task cannot depend on other tasks in the same TaskTemplate or Array. "
                f"Task={task} has dependencies already added to {self}. Check for upstream and"
                f" downstream tasks in array.tasks or workflow.tasks that have the following "
                f"hashes. {[hash(task) for task in cyclical_tasks]}"
            )

    def bind(self) -> None:
        """Add an array to the database."""
        app_route = "/array"
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "task_template_version_id": self.task_template_version.id,
                "workflow_id": self.workflow.workflow_id,
                "max_concurrently_running": self.max_concurrently_running,
                "name": self.name,
            },
            request_type="post",
        )

        if rc != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {resp}"
            )

        array_id = resp["array_id"]
        self.array_id = array_id
