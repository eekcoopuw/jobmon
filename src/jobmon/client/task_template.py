"""A framework that many tasks have in common while varying by a declared set of arguments."""
from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.task import Task
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientTaskTemplate

logger = logging.getLogger(__name__)


class TaskTemplate:
    """Task Template outlines the structure of a Task to give it more context within the DAG.

    A Task Template defines a framework that many tasks have in common while varying by a
    declared set of arguments.
    """

    def __init__(self, template_name: str, requester: Optional[Requester] = None) -> None:
        """Groups tasks of a type.

        Declares the concrete arguments that instances may vary over either from workflow to
        workflow or between nodes in the stage of a dag.

        Args:
            template_name: the name of this task template.
            requester: object to communicate with the flask services.
        """
        self.template_name = template_name

        # versions
        self._task_template_versions: List[TaskTemplateVersion] = []
        self._active_task_template_version: TaskTemplateVersion

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    @classmethod
    def get_task_template(cls: Any, tool_version_id: int, template_name: str,
                          requester: Optional[Requester] = None) -> TaskTemplate:
        """Get a bound instance of TaskTemplate.

        Args:
            tool_version_id: tool version to associate this task template with
            template_name: name of this specific task template
            requester: object to communicate with the flask services.
        """
        task_template = cls(template_name, requester)
        task_template.bind(tool_version_id)
        return task_template

    @property
    def default_cluster_name(self) -> str:
        """Default cluster_name associated with active tool version."""
        return self.active_task_template_version.default_cluster_name

    @default_cluster_name.setter
    def default_cluster_name(self, cluster_name: str) -> None:
        """Set default cluster.

        Args:
            cluster_name: name of cluster to set as default.
        """
        self.active_task_template_version.default_cluster_name = cluster_name

    @property
    def default_compute_resources_set(self) -> Dict[str, Dict[str, Any]]:
        """Default compute resources associated with active tool version."""
        return self.active_task_template_version.default_compute_resources_set

    def update_default_compute_resources(self, cluster_name: str, **kwargs: Any) -> None:
        """Update default compute resources in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        self.active_task_template_version.update_default_compute_resources(cluster_name,
                                                                           **kwargs)

    def set_default_compute_resources_from_yaml(self, cluster_name: str, yaml_file: str) \
            -> None:
        """Set default ComputeResources from a user provided yaml file for task template level.

        TODO: Implement this method.

        Args:
            cluster_name: name of cluster to set default values for.
            yaml_file: the yaml file that is providing the compute resource values.
        """
        pass

    def set_default_compute_resources_from_dict(self, cluster_name: str,
                                                compute_resources: Dict[str, Any]) -> None:
        """Set default compute resources for a given cluster_name.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to set default values for.
            compute_resources: dictionary of default compute resources to run tasks
                with. Can be overridden at task template or task level.
                dict of {resource_name: resource_value}
        """
        self.active_task_template_version.set_default_compute_resources_from_dict(
            cluster_name, compute_resources
        )

    @classmethod
    def from_wire(cls: Any, wire_tuple: Tuple, requester: Optional[Requester] = None
                  ) -> TaskTemplate:
        """Get a bound instance of TaskTemplate from the http wire format.

        Args:
            wire_tuple: Wire format for ToolVersion defined in jobmon.serializers.
            requester: communicate with the flask services.
        """
        task_template_kwargs = SerializeClientTaskTemplate.kwargs_from_wire(wire_tuple)
        task_template = cls(template_name=task_template_kwargs["template_name"],
                            requester=requester)
        task_template._task_template_id = task_template_kwargs["id"]
        task_template.tool_version_id = task_template_kwargs["tool_version_id"]
        return task_template

    def bind(self, tool_version_id: int) -> None:
        """Bind task template to the db.

        Args:
            tool_version_id: the version of the tool this task template is associated with.
        """
        if self.is_bound:
            return

        app_route = "/client/task_template"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tool_version_id": tool_version_id,
                     "task_template_name": self.template_name},
            request_type='post',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        self._task_template_id = response["task_template_id"]
        self.tool_version_id = tool_version_id

    @property
    def is_bound(self) -> bool:
        """If the task template version has been bound to the database."""
        return hasattr(self, "_task_template_id")

    @property
    def id(self) -> int:
        """Unique id from db if task_template has been bound."""
        if not self.is_bound:
            raise AttributeError(
                "Cannot access id until TaskTemplate is bound to a tool version"
            )
        return self._task_template_id

    @property
    def task_template_versions(self) -> List[TaskTemplateVersion]:
        """Version of task template if it has been bound."""
        return self._task_template_versions

    @property
    def active_task_template_version(self) -> TaskTemplateVersion:
        """The TaskTemplateVersion to use when spawning tasks."""
        if not self.task_template_versions:
            raise AttributeError(
                "Cannot access attribute active_task_template_version because there are no "
                f"TaskTemplateVersions associated with task_template_name={self.template_name}"
                ". Either create some using get_task_template_version or load existing ones "
                "from the database using load_task_template_versions."
            )
        return self._active_task_template_version

    def set_active_task_template_version_id(self, task_template_version_id: Union[str, int]
                                            = "latest") -> None:
        """The TaskTemplateVersion that is set as the active one (latest is default).

        Args:
            task_template_version_id: which version to set as active on this object.
        """
        if not self.task_template_versions:
            raise AttributeError(
                "Cannot set an active_task_template_version until task templates versions "
                f"exist for task_template_name={self.template_name}. Either create some using "
                " get_task_template_version or load existing ones from the database using "
                "load_task_template_versions."
            )
        version_index_lookup = {self.task_template_versions[index].id: index
                                for index in range(len(self.task_template_versions))
                                if self.task_template_versions[index].is_bound}

        # get the lookup value
        if task_template_version_id == "latest":
            lookup_version: int = int(max(version_index_lookup.keys()))
        else:
            lookup_version = int(task_template_version_id)

        # check that the version exists
        try:
            version_index = version_index_lookup[lookup_version]
        except KeyError:
            raise ValueError(
                f"task_template_version_id {task_template_version_id} is not a valid "
                f"version for task_template_name={self.template_name} and tool_version_id="
                f"{self.tool_version_id}. Valid versions={version_index_lookup.keys()}")

        self._active_task_template_version = self.task_template_versions[version_index]

    def set_active_task_template_version(self, task_template_version: TaskTemplateVersion) \
            -> None:
        """The TaskTemplateVersion that is set as the active one.

        Args:
            task_template_version: which version to set as active on this object.
        """
        # build a lookup between the version list and the version hash
        hash_index_lookup = {hash(self.task_template_versions[index]): index
                             for index in range(len(self.task_template_versions))}

        # get the lookup value
        lookup_hash = hash(task_template_version)

        # check that the version exists
        try:
            version_index = hash_index_lookup[lookup_hash]
        except KeyError:
            self.task_template_versions.append(task_template_version)
            version_index = len(self.task_template_versions) - 1
            if version_index < 0:
                version_index = 0
            hash_index_lookup[lookup_hash] = version_index

        self._active_task_template_version = self.task_template_versions[version_index]

    def load_task_template_versions(self) -> None:
        """Load task template versions associated with this task template from the database."""
        app_route = f"/client/task_template/{self.id}/versions"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        task_template_versions = [TaskTemplateVersion.from_wire(wire_args) for wire_args in
                                  response["task_template_versions"]]
        self._task_template_versions = task_template_versions

        # activate the latest version
        if self.task_template_versions:
            self.set_active_task_template_version_id()

    def get_task_template_version(self, command_template: str, node_args: List[str] = [],
                                  task_args: List[str] = [], op_args: List[str] = [],
                                  default_cluster_name: str = "",
                                  default_compute_resources: Optional[Dict[str, Any]] = None
                                  ) -> TaskTemplateVersion:
        """Create a task template version instance. If it already exists, activate it.

        Args:
            command_template: an abstract command representing a task, where the arguments to
                the command have defined names but the values are not assigned.
                eg: '{python} {script} --data {data} --para {para} {verbose}'
            node_args: any named arguments in command_template that make the command unique
                within this template for a given workflow run. Generally these are arguments
                that can be parallelized over.
            task_args: any named arguments in command_template that make the command unique
                across workflows if the node args are the same as a previous workflow.
                Generally these are arguments about data moving though the task.
            op_args: any named arguments in command_template that can change without changing
                the identity of the task. Generally these are things like the task executable
                location or the verbosity of the script.
            default_cluster_name: the default cluster to run each task associated with this
                template on.
            default_compute_resources: dictionary of default compute resources to run tasks
                with. Can be overridden at task level. dict of {resource_name: resource_value}.
                Must specify default_cluster_name when this option is used.
        """
        if default_compute_resources is not None and not default_cluster_name:
            raise ValueError(
                "Must specify default_cluster_name when using default_compute_resources option"
            )

        if not node_args:
            node_args = []
        if not task_args:
            task_args = []
        if not op_args:
            op_args = []

        task_template_version = TaskTemplateVersion(
            command_template=command_template,
            node_args=node_args,
            task_args=task_args,
            op_args=op_args,
            requester=self.requester
        )

        # now activate it
        self.set_active_task_template_version(task_template_version)
        self.active_task_template_version.bind(self.id)

        # set compute resources on the newly active task template version if specified
        self.active_task_template_version.default_cluster_name = default_cluster_name
        if default_compute_resources:
            self.active_task_template_version.set_default_compute_resources_from_dict(
                default_cluster_name, default_compute_resources
            )

        return self.active_task_template_version

    def create_task(self,
                    name: Optional[str] = None,
                    upstream_tasks: List[Task] = [],
                    task_attributes: Union[List, dict] = {},
                    max_attempts: int = 3,
                    compute_resources: Optional[Dict[str, Any]] = None,
                    compute_resources_callable: Optional[Callable] = None,
                    resource_scales: Optional[Dict[str, Any]] = None,
                    cluster_name: str = "",
                    **kwargs: Any) -> Task:
        """Create an instance of a task associated with this template.

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

            **kwargs: values for each argument specified in command_template

        Returns:
            ExecutableTask

        Raises:
            ValueError: if the args that are supplied do not match the args in the command
                template.
        """
        # make sure task template is bound to tool version
        if not self.is_bound:
            raise RuntimeError(f"TaskTemplate={self.template_name} must be bound to a tool "
                               "version before tasks can be created.")

        # bind task template version to task template if needed
        if not self.active_task_template_version.is_bound:
            self.active_task_template_version.bind(self.id)

        # if we have argument overlap
        if "name" in self.active_task_template_version.template_args:
            kwargs["name"] = name

        # kwargs quality assurance
        if self.active_task_template_version.template_args != set(kwargs.keys()):
            raise ValueError(
                f"Unexpected kwarg. Task Template requires "
                f"{self.active_task_template_version.template_args}, got {set(kwargs.keys())}"
            )

        command = self.active_task_template_version.command_template.format(**kwargs)

        # arg id name mappings
        node_args = {self.active_task_template_version.id_name_map[k]: str(v)
                     for k, v in kwargs.items()
                     if k in self.active_task_template_version.node_args}
        task_args = {self.active_task_template_version.id_name_map[k]: str(v)
                     for k, v in kwargs.items()
                     if k in self.active_task_template_version.task_args}

        # set cluster_name, function level overrides default
        if not cluster_name:
            cluster_name = self.default_cluster_name

        # Set compute resources, task compute resources override tasktemplate defaults
        if compute_resources is None:
            compute_resources = {}
        resources = self.default_compute_resources_set.get(cluster_name, {}).copy()
        resources.update(compute_resources)

        # build task
        task = Task(
            command=command,
            task_template_version_id=self.active_task_template_version.id,
            node_args=node_args,
            task_args=task_args,
            compute_resources=resources,
            compute_resources_callable=compute_resources_callable,
            resource_scales=resource_scales,
            cluster_name=cluster_name,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes,
            requester=self.requester
        )
        return task

    def __hash__(self) -> int:
        """A hash of the TaskTemplate name and tool version concatenated together."""
        hash_value = int(hashlib.sha1(self.template_name.encode('utf-8')).hexdigest(), 16)
        return hash_value
