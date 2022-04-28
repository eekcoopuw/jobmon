"""Task object defines a single executable object that will be added to a Workflow.

TaskInstances will be created from it for every execution.
"""
from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.node import Node
from jobmon.client.task_resources import TaskResources
from jobmon.constants import SpecialChars, TaskStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeTaskInstanceErrorLog, SerializeTaskResourceUsage

if TYPE_CHECKING:
    from jobmon.client.array import Array
    from jobmon.client.workflow import Workflow

logger = logging.getLogger(__name__)


class Task:
    """Task object defines a single executable object that will be added to a Workflow.

    Task Instances will be created from it for every execution.
    """

    @staticmethod
    def is_valid_job_name(name: str):
        """If the name is invalid it will raises an exception.

        Primarily based on the restrictions SGE places on job names. The list of illegal
        characters might not be complete, I could not find an official list.

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
            raise ValueError(f"name cannot begin with a digit, saw: '{name[0]}'")
        elif any(e in name for e in SpecialChars.ILLEGAL_SPECIAL_CHARACTERS):
            raise ValueError(
                f"name contains illegal special character, illegal characters "
                f"are: '{SpecialChars.ILLEGAL_SPECIAL_CHARACTERS}'"
            )
        return True

    def __init__(
        self,
        node: Node,
        task_args: Dict[str, Any],
        op_args: Dict[str, Any],
        array: Optional[Array] = None,
        cluster_name: str = "",
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, float]] = None,
        fallback_queues: Optional[List[str]] = None,
        name: Optional[str] = None,
        max_attempts: int = 3,
        upstream_tasks: Optional[List[Task]] = None,
        task_attributes: Union[List, dict] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """Create a single executable object in the workflow, aka a Task.

        Relate it to a Task Template in order to classify it as a type of job within the
        context of your workflow.

        Args:
            command: the unique command for this Task, also readable by humans. Should
                include all parameters. Two Tasks are equal (__eq__) iff they have the same
                command.
            node: Node this task is associated with.
            task_args: Task arguments that make the command unique across workflows
                usually pertaining to data flowing through the task.
            task_args: Task arguments that can change across runs of the same workflow.
                usually pertaining to trivial things like log level or code location.
            cluster_name: the name of the cluster the user wants to run their task on.
            compute_resources: A dictionary that includes the users requested resources
                for the current run. E.g. {cores: 1, mem: 1, runtime: 60, queue: all.q}.
            compute_resources_callable: callable compute resources.
            resource_scales: how much users want to scale their resource request if the
                the initial request fails.
            fallback_queues: a list of queues that a user wants to try if their original
                queue is unable to accommodate their requested resources.
            name: name that will be visible in qstat for this job
            max_attempts: number of attempts to allow the cluster to try before giving
                up. Default is 3.
            upstream_tasks: Task objects that must be run prior to this
            task_attributes: dictionary of attributes and their values or list
                of attributes that will be assigned later.
            requester: requester object to communicate with the flask services.

        Raise:
            ValueError: If the hashed command is not allowed as an SGE job name; see
                is_valid_job_name
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # pre bind hash defining attributes
        self.node = node
        self.task_args = task_args
        self.mapped_task_args = (
            self.node.task_template_version.convert_arg_names_to_ids(**self.task_args)
        )
        self.task_args_hash = self._hash_task_args()
        self.op_args = op_args

        # pre bind mutable attributes
        self.command = self.node.task_template_version.command_template.format(
            **self.node.node_args, **self.task_args, **self.op_args
        )

        # Not all tasks bound to an array initially
        if array is not None:
            self.array = array

        # Names of jobs can't start with a numeric.
        if not name:
            name = self.node.default_name
        self.is_valid_job_name(name)
        self.name = name

        # upstream and downstream task relationships
        self.upstream_tasks: Set[Task] = (
            set(upstream_tasks) if upstream_tasks else set()
        )
        self.downstream_tasks: Set[Task] = set()
        for task in self.upstream_tasks:
            self.add_upstream(task)

        self.task_attributes: dict = {}
        if isinstance(task_attributes, List):
            for attr in task_attributes:
                self.task_attributes[attr] = None
        elif isinstance(task_attributes, dict):
            for attr in task_attributes:
                self.task_attributes[str(attr)] = str(task_attributes[attr])
        else:
            raise ValueError(
                "task_attributes must be provided as a list of attributes or a "
                "dictionary of attributes and their values"
            )

        # mutable operational/cluster behaviour
        self.max_attempts: int = max_attempts
        self._instance_cluster_name = cluster_name
        self._instance_compute_resources = (
            compute_resources if compute_resources is not None else {}
        )
        self._instance_compute_resources_callable = compute_resources_callable
        self.resource_scales: Dict[str, float] = (
            resource_scales
            if resource_scales is not None
            else {"memory": 0.5, "runtime": 0.5}
        )
        self.fallback_queues: List[str] = (
            fallback_queues if fallback_queues is not None else []
        )

        # error api
        self._errors: Union[
            None, Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]
        ] = None

    @property
    def compute_resources(self) -> Dict[str, Any]:
        """A dictionary that includes the users requested resources for the current run.

        E.g. {cores: 1, mem: 1, runtime: 60, queue: all.q}"""
        try:
            resources = self.array.compute_resources
        except AttributeError:
            resources = {}
        resources.update(self._instance_compute_resources.copy())
        return resources

    @property
    def cluster_name(self) -> str:
        """The name of the cluster the user wants to run their task on."""
        cluster_name = self._instance_cluster_name
        if not cluster_name:
            try:
                cluster_name = self.array.cluster_name
            except AttributeError:
                # array hasn't been inferred yet. safe to return empty string for now
                pass
        return cluster_name

    @property
    def compute_resources_callable(self) -> Optional[Callable]:
        """A callable that returns a compute resources dict."""
        compute_resources_callable = self._instance_compute_resources_callable
        if compute_resources_callable is None:
            compute_resources_callable = self.array.compute_resources_callable
        return compute_resources_callable

    @property
    def original_task_resources(self) -> TaskResources:
        """Get the id of the task if it has been bound to the db otherwise raise an error."""
        if not hasattr(self, "_original_task_resources"):
            raise AttributeError(
                "task_resources cannot be accessed before workflow is bound"
            )
        return self._original_task_resources

    @original_task_resources.setter
    def original_task_resources(self, val: TaskResources) -> None:
        if not isinstance(val, TaskResources):
            raise ValueError("task_resources must be of type=TaskResources")
        self._original_task_resources = val

    @property
    def is_bound(self) -> bool:
        """If the task template version has been bound to the database."""
        return hasattr(self, "_task_id")

    @property
    def task_id(self) -> int:
        """Get the id of the task if it has been bound to the db otherwise raise an error."""
        if not self.is_bound:
            raise AttributeError("task_id cannot be accessed before task is bound")
        return self._task_id

    @task_id.setter
    def task_id(self, val: int) -> None:
        self._task_id = val

    @property
    def initial_status(self) -> str:
        """Get initial status of the task if it has been bound to the db; else raise error."""
        if not hasattr(self, "_initial_status"):
            raise AttributeError(
                "initial_status cannot be accessed before task is bound"
            )
        return self._initial_status

    @initial_status.setter
    def initial_status(self, val: str) -> None:
        self._initial_status = val

    @property
    def final_status(self) -> str:
        """Get initial status of the task if it has been bound, otherwise raise error."""
        if not hasattr(self, "_final_status"):
            raise AttributeError(
                "final_status cannot be accessed until workflow is run"
            )
        return self._final_status

    @final_status.setter
    def final_status(self, val: str) -> None:
        self._final_status = val

    @property
    def array(self) -> Array:
        """Get the array the task has been added to or else raise an AttributeError."""
        if not hasattr(self, "_array"):
            raise AttributeError(
                "array cannot be accessed via task before task is added to array"
            )
        return self._array

    @array.setter
    def array(self, val: Array) -> None:
        self._array = val

    @property
    def workflow(self) -> Workflow:
        """Get the workflow the task has been added to or else raise an AttributeError."""
        if not hasattr(self, "_workflow"):
            raise AttributeError(
                "workflow cannot be accessed via task before workflow is added to workflow"
            )
        return self._workflow

    @workflow.setter
    def workflow(self, val: Workflow) -> None:
        self._workflow = val

    def bind(self, reset_if_running: bool = True) -> int:
        """Bind tasks to the db if they have not been bound already.

        Otherwise make sure their ExecutorParameters are up to date.
        """
        task_id, status = self._get_task_id_and_status()
        if task_id is None:
            task_id = self._add_task()
            status = TaskStatus.REGISTERING
        else:
            status = self._update_task_parameters(task_id, reset_if_running)
        self._task_id = task_id
        self._initial_status = status
        return task_id

    def add_upstream(self, ancestor: Task) -> None:
        """Add an upstream (ancestor) Task.

        This has Set semantics, an upstream task will only be added once. Symmetrically, this
        method also adds this Task as a downstream on the ancestor.
        """
        self.upstream_tasks.add(ancestor)
        ancestor.downstream_tasks.add(self)

        self.node.add_upstream_node(ancestor.node)

    def add_downstream(self, descendent: Task) -> None:
        """Add an downstream (ancestor) Task.

        This has Set semantics, a downstream task will only be added once. Symmetrically,
        this method also adds this Task as an upstream on the ancestor.
        """
        self.downstream_tasks.add(descendent)
        descendent.upstream_tasks.add(self)

        self.node.add_downstream_node(descendent.node)

    def add_attributes(self, task_attributes: dict) -> None:
        """Update or add attributes.

        Function that users can call either to update values of existing attributes or add
        new attributes.
        """
        app_route = f"/task/{self.task_id}/task_attributes"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"task_attributes": task_attributes},
            request_type="put",
            logger=logger,
        )
        if return_code != StatusCodes.OK:
            raise ValueError(
                f"Unexpected status code {return_code} from PUT request through "
                f"route {app_route}. Expected code 200. Response content: "
                f"{response}"
            )

    def add_attribute(self, attribute: str, value: str) -> None:
        """Function that users can call to add a single attribute for a task."""
        self.task_attributes[str(attribute)] = str(value)
        # if the task has already been bound, bind the attributes
        if self._task_id:
            self.add_attributes({str(attribute): str(value)})

    def get_errors(
        self,
    ) -> Union[None, Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]]:
        """Return all errors for each task, with the recent task_instance_id actually used."""
        if (
            self._errors is None
            and hasattr(self, "_task_id")
            and self._task_id is not None
        ):
            return_code, response = self.requester.send_request(
                app_route=f"/task/{self._task_id}/most_recent_ti_error",
                message={},
                request_type="get",
                logger=logger,
            )
            if return_code == StatusCodes.OK:
                task_instance_id = response["task_instance_id"]
                if task_instance_id is not None:
                    rc, response = self.requester.send_request(
                        app_route=f"/task_instance/{task_instance_id}"
                        f"/task_instance_error_log",
                        message={},
                        request_type="get",
                    )
                    errors_ti = [
                        SerializeTaskInstanceErrorLog.kwargs_from_wire(j)
                        for j in response["task_instance_error_log"]
                    ]
                    self._errors = {
                        "task_instance_id": task_instance_id,
                        "error_log": errors_ti,
                    }

        return self._errors

    def set_compute_resources_from_yaml(
        self, cluster_name: str, yaml_file: str
    ) -> None:
        """Set default compute resources from a user provided yaml file for task level.

        TODO: Implement this method.

        Args:
            cluster_name: name of cluster to set default values for.
            yaml_file: the yaml file that is providing the compute resource values.
        """
        pass

    def update_compute_resources(self, **kwargs: Any) -> None:
        """Function that allows users to update their compute resources."""
        self.compute_resources.update(kwargs)

    def _hash_task_args(self) -> int:
        """A hash of the encoded result of the args and values concatenated together."""
        arg_ids = list(self.mapped_task_args.keys())
        arg_ids.sort()

        arg_values = [str(self.mapped_task_args[key]) for key in arg_ids]
        str_arg_ids = [str(arg) for arg in arg_ids]

        hash_value = int(
            hashlib.sha1("".join(str_arg_ids + arg_values).encode("utf-8")).hexdigest(),
            16,
        )
        return hash_value

    def _get_task_id_and_status(self) -> Tuple[Optional[int], Optional[str]]:
        """Get the id and status for a task from the db."""
        app_route = "/task"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow.workflow_id,
                "node_id": self.node.node_id,
                "task_args_hash": self.task_args_hash,
            },
            request_type="get",
            logger=logger,
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from GET "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )
        return response["task_id"], response["task_status"]

    def _update_task_parameters(self, task_id: int, reset_if_running: bool) -> str:
        """Update the executor parameters in the db for a task."""
        app_route = f"/task/{task_id}/update_parameters"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "name": self.name,
                "command": self.command,
                "max_attempts": self.max_attempts,
                "reset_if_running": reset_if_running,
                "task_attributes": self.task_attributes,
            },
            request_type="put",
            logger=logger,
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from PUT request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        return response["task_status"]

    def _add_task(self) -> int:
        """Bind a task to the db with the node, and workflow ids that have been established."""
        tasks = [
            {
                "workflow_id": self.workflow.workflow_id,
                "node_id": self.node.node_id,
                "array_id": self.array.array_id,
                "task_args_hash": self.task_args_hash,
                "name": self.name,
                "command": self.command,
                "max_attempts": self.max_attempts,
                "task_args": self.mapped_task_args,
                "task_attributes": self.task_attributes,
            }
        ]
        app_route = "/task"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tasks": tasks},
            request_type="post",
            logger=logger,
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        return list(response["tasks"].values())[0]

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, Task):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: Task) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)

    def __hash__(self) -> int:
        """Create the hash for a task to determine if it is unique within a dag."""
        if not hasattr(self, "_hash_val"):
            hash_value = hashlib.sha1()
            hash_value.update(bytes(str(hash(self.node)).encode("utf-8")))
            hash_value.update(bytes(str(self.task_args_hash).encode("utf-8")))
            self._hash_val = int(hash_value.hexdigest(), 16)
        return self._hash_val

    def __repr__(self) -> str:
        """A representation string for a Task instance."""
        repr_string = (
            f"Task(command={self.command}, "
            f"name={self.name}, "
            f"node={self.node}, "
            f"task_args={self.task_args}"
        )
        try:
            repr_string += f", task_id={self.task_id})"
        except AttributeError:
            repr_string += ")"
        return repr_string

    def resource_usage(self) -> dict:
        """Get the resource usage for the successful TaskInstance of a Task."""
        app_route = "/task_resource_usage"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"task_id": self.task_id},
            request_type="get",
            logger=logger,
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from GET "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )
        return SerializeTaskResourceUsage.kwargs_from_wire(response)
