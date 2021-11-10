from http import HTTPStatus as StatusCodes
from itertools import product
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class Array:
    """Representation of a client array object.

    Supports functionality to create tasks from the cross product of provided node_args.
    """

    def __init__(
        self,
        task_template_name: str,
        task_template_version: TaskTemplateVersion,
        max_concurrently_running: int,
        cluster_name: str,
        task_args: Dict[str, Any],
        op_args: Dict[str, Any],
        name: str = "",
        max_attempts: int = 3,
        threshold_to_submit: int = 100,
        upstream_tasks: Optional[List["Task"]] = None,
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialize the array object."""
        self.task_template_name = task_template_name
        self.task_template_version = task_template_version
        self.max_concurrently_running = max_concurrently_running
        self.task_args = task_args
        self.op_args = op_args
        self.name = name
        self.max_attempts = max_attempts
        self.threshold_to_submit = threshold_to_submit
        if upstream_tasks is None:
            upstream_tasks = []
        self.upstream_tasks = upstream_tasks
        if compute_resources is None:
            compute_resources = task_template_version.default_compute_resources_set
        self.default_compute_resources_set = compute_resources
        self.compute_resources_callable = compute_resources_callable
        self.resource_scales = resource_scales
        self.default_cluster_name = cluster_name
        self._task_resources: Optional[TaskResources] = None  # Initialize to None
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester
        self.tasks: List["Task"] = []  # Initialize to empty list

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
                "Task resource must be bound and have an ID to be assigned"
                "to the array."
            )
        self._task_resources = task_resources

    @property
    def task_args_mapped(self) -> Dict[int, Any]:
        """Map the task args to the arg IDs from the database."""
        if not hasattr(self, "_task_args_mapped"):
            task_args_mapped = {
                self.task_template_version.id_name_map[k]: str(v)
                for k, v in self.task_args.items()
            }
            self._task_args_mapped = task_args_mapped

        return self._task_args_mapped

    def set_task_resources(self, task_resources: TaskResources) -> None:
        """Set the task resources for the array and all its constituent tasks."""
        if self._task_resources is not None:
            logger.warning(
                "Task resources have already been set on this array, updating"
            )
        self.task_resources = task_resources
        for task in self.tasks:
            task.task_resources = task_resources

    def create_tasks(
        self,
        name: Optional[str] = None,
        upstream_tasks: Optional[List["Task"]] = None,
        task_attributes: Union[List, dict] = {},
        max_attempts: int = 3,
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        cluster_name: str = "",
        **node_kwargs: Any,
    ) -> List["Task"]:
        """Create an task associated with the array.

        Args:
            name: a name associated with this specific task
            upstream_tasks: Task objects that must be run prior to this one
            task_attributes (dict or list): attributes and their values or just the attributes
                that will be given values later
            max_attempts: Number of attempts to try this task before giving up. Default is 3.
            cluster_name: name of cluster to run task on.
            compute_resources: dictionary of default compute resources to run tasks
                with. Can be overridden at task template or task level.
                dict of {resource_name: resource_value}
            compute_resources_callable: a function that can dynamically generate compute
                resources when a task's resources are adjusted
            resource_scales: determines the scaling factor for how aggressive resource
                adjustments will be scaled up
            **node_kwargs: values for each node argument specified in command_template

        Returns:
            ExecutableTask

        Raises:
            ValueError: if the args that are supplied do not match the args in the command
                template.
        """
        if name is None:
            name = self.name

        if upstream_tasks is None:
            # If not specified, defined from the array upstreams
            upstream_tasks = self.upstream_tasks

        # Expand the node_args
        node_args_expanded = Array.expand_dict(**node_kwargs)

        # set cluster_name, function level overrides default
        if cluster_name is None:
            cluster_name = self.default_cluster_name

        # Set compute resources, task compute resources override array defaults
        if compute_resources is None:
            compute_resources = {}

        if compute_resources_callable is None:
            compute_resources_callable = self.compute_resources_callable

        resources = self.default_compute_resources_set.copy()
        resources.update(compute_resources)

        # build tasks over node_args
        tasks = []
        for node_arg in node_args_expanded:
            # Template and map to IDs
            task_arguments = dict(**node_arg, **self.task_args, **self.op_args)
            command = self.task_template_version.command_template.format(
                **task_arguments
            )

            node_args_mapped = {
                self.task_template_version.id_name_map[k]: v
                for k, v in node_arg.items()
            }

            task = Task(
                command=command,
                task_template_version_id=self.task_template_version.id,
                node_args=node_args_mapped,
                task_args=self.task_args_mapped,
                compute_resources=resources,
                compute_resources_callable=compute_resources_callable,
                resource_scales=resource_scales,
                cluster_name=cluster_name,
                name=name,
                max_attempts=max_attempts,
                upstream_tasks=upstream_tasks,
                task_attributes=task_attributes,
                requester=self.requester,
            )
            tasks.append(task)
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
        node_args_mapped = {
            self.task_template_version.id_name_map[k]: v for k, v in kwargs.items()
        }
        for task in self.tasks:
            node_args_mapped_minus_task = dict(
                node_args_mapped.items() - task.node.node_args.items()
            )
            if len(node_args_mapped_minus_task) == 0:
                tasks.append(task)
        return tasks

    def bind(self, workflow_id: int, cluster_id: int) -> None:
        """Add an array to the database."""
        app_route = "/array"
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "task_template_version_id": self.task_template_version.id,
                "workflow_id": workflow_id,
                "max_concurrently_running": self.max_concurrently_running,
                "threshold_to_submit": self.threshold_to_submit,
                # assume num_completed always null on bind, computed later in distributor
                "num_completed": None,
                "task_resources_id": self.task_resources.id,
                "cluster_id": cluster_id,
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

        # Assign array_id to all tasks
        for task in self.tasks:
            task.array_id = array_id
