"""Tool represents a project or model that will be run many times over.

The Tool may evolve over time.
"""
from __future__ import annotations

import getpass
from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Dict, List, Optional, Union

import yaml

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template import TaskTemplate
from jobmon.client.tool_version import ToolVersion
from jobmon.client.workflow import Workflow
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientTool

logger = logging.getLogger(__name__)


class InvalidToolError(Exception):
    """Exception for Tools that do not exist in the DB."""

    pass


class InvalidToolVersionError(Exception):
    """Exception for Tool version that is not valid."""

    pass


class Tool:
    """Tool represents a project or model that will be run many times over.

    The Tool may evolve over time.
    """

    def __init__(
        self,
        name: str = f"unknown-{getpass.getuser()}",
        active_tool_version_id: Union[str, int] = "latest",
        requester: Optional[Requester] = None,
    ) -> None:
        """A tool is an application which is expected to run many times on variable inputs.

         Which will serve a certain purpose over time even as the internal pipeline may change.
         Example tools are Dismod, Burdenator, Codem.

        Args:
            name: the name of the tool
            active_tool_version_id: which version of the tool to attach task templates and
                workflows to.
            requester: communicate with the flask services.
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # set tool defining attributes
        self.name = name
        self._bind()

        # import tool versions
        self.tool_versions = self._load_tool_versions()
        if not self.tool_versions:
            self.get_new_tool_version()
        else:
            self.set_active_tool_version_id(active_tool_version_id)

    def get_new_tool_version(self) -> int:
        """Create a new tool version for the current tool and activate it.

        Returns: the version id for the new tool
        """
        # call route to create tool version

        tool_version = ToolVersion.get_tool_version(
            tool_id=self.id, requester=self.requester
        )
        tool_version_id = tool_version.id
        self.tool_versions.append(tool_version)
        self.set_active_tool_version_id(tool_version_id)
        return tool_version_id

    @property
    def active_task_templates(self) -> Dict[str, TaskTemplate]:
        """Mapping of template_name to TaskTemplate for the active tool version."""
        return self.active_tool_version.task_templates

    @property
    def active_tool_version(self) -> ToolVersion:
        """Tool version id to use when spawning task templates."""
        return self._active_tool_version

    @property
    def default_compute_resources_set(self) -> Dict[str, Dict[str, Any]]:
        """Default compute resources associated with active tool version."""
        return self.active_tool_version.default_compute_resources_set

    @property
    def default_cluster_name(self) -> str:
        """Default cluster_name associated with active tool version."""
        return self.active_tool_version.default_cluster_name

    def set_active_tool_version_id(self, tool_version_id: Union[str, int]) -> None:
        """Tool version that is set as the active one (latest is default during instantiation).

        Args:
            tool_version_id: which tool version to set as active on this object.
        """
        version_index_lookup = {
            self.tool_versions[index].id: index
            for index in range(len(self.tool_versions))
        }

        # get the lookup value
        if tool_version_id == "latest":
            lookup_version: int = int(max(version_index_lookup.keys()))
        else:
            lookup_version = int(tool_version_id)

        # check that the version exists
        try:
            version_index = version_index_lookup[lookup_version]
        except KeyError:
            raise ValueError(
                f"{tool_version_id} is not a valid version for tool.name={self.name} Valid "
                f"versions={version_index_lookup.keys()}"
            )

        # set it as active and load task templates
        tool_version = self.tool_versions[version_index]
        tool_version.load_task_templates()
        self._active_tool_version: ToolVersion = tool_version

    def get_task_template(
        self,
        template_name: str,
        command_template: str,
        node_args: List[str] = None,
        task_args: List[str] = None,
        op_args: List[str] = None,
        default_cluster_name: str = "",
        default_compute_resources: Optional[Dict[str, Any]] = None,
        yaml_file: str = None,
    ) -> TaskTemplate:
        """Create or get task a task template.

        Args:
            template_name: the name of this task template.
            command_template: an abstract command representing a task, where the arguments to
                the command have defined names but the values are not assigned. eg: '{python}
                {script} --data {data} --para {para} {verbose}'
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
            yaml_file: path to YAML file that contains user-specified compute resources.
        """
        if node_args is None:
            node_args = []
        if task_args is None:
            task_args = []
        if op_args is None:
            op_args = []

        if default_compute_resources is not None and not default_cluster_name:
            raise ValueError(
                "Must specify default_cluster_name when using default_compute_resources option"
            )

        tt = self.active_tool_version.get_task_template(template_name)

        # Read in compute resources from YAML
        if yaml_file and default_compute_resources is None:
            with open(yaml_file, "r") as stream:
                try:
                    default_compute_resources = yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    raise Exception(
                        f"Unable to read resources from {yaml_file}. "
                        f"Exception: {exc}"
                    )
            default_compute_resources = default_compute_resources[
                "task_template_resources"
            ][tt.template_name][default_cluster_name]
        tt.get_task_template_version(
            command_template,
            node_args,
            task_args,
            op_args,
            default_cluster_name,
            default_compute_resources,
        )
        return tt

    def create_workflow(
        self,
        workflow_args: str = "",
        name: str = "",
        description: str = "",
        workflow_attributes: Optional[Union[List, dict]] = None,
        max_concurrently_running: int = 10_000,
        chunk_size: int = 500,
        default_cluster_name: str = "",
        default_compute_resources_set: Optional[Dict] = None,
    ) -> Workflow:
        """Create a workflow object associated with the active tool version.

        Args:
            workflow_args: Unique identifier of a workflow.
            name: Name of the workflow.
            description: Description of the workflow.
            workflow_attributes: Any key/value pair that the user wants to record for this
                workflow
            max_concurrently_running: How many running jobs to allow in parallel.
            chunk_size: how many tasks to bind in a single request
            default_cluster_name: name of cluster to run tasks on by default. Can be overridden
                at the task template or task level.
            default_compute_resources_set: dictionary of default compute resources to run tasks
                with. Can be overridden at task template or task level.
                dict of {cluster_name: {resource_name: resource_value}}
        """
        wf = Workflow(
            self.active_tool_version.id,
            workflow_args,
            name,
            description,
            workflow_attributes,
            max_concurrently_running,
            requester=self.requester,
            chunk_size=chunk_size,
        )

        # set compute resource defaults
        if default_cluster_name:
            wf.default_cluster_name = default_cluster_name
        else:
            if self.default_cluster_name:
                wf.default_cluster_name = self.default_cluster_name
        if default_compute_resources_set:
            wf.default_compute_resources_set = default_compute_resources_set
        else:
            if self.active_tool_version.default_compute_resources_set:
                wf.default_compute_resources_set = self.default_compute_resources_set

        return wf

    def update_default_compute_resources(
        self, cluster_name: str, **kwargs: Any
    ) -> None:
        """Update default compute resources in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        if not self.default_cluster_name:
            self.active_tool_version.default_cluster_name = cluster_name
        self.active_tool_version.update_default_compute_resources(
            cluster_name, **kwargs
        )

    def set_default_compute_resources_from_yaml(
        self,
        default_cluster_name: str,
        yaml_file: str,
        set_task_templates: bool = False,
        ignore_missing_keys: bool = False,
    ) -> None:
        """Set default compute resources from a user provided yaml file for tool level.

        Args:
            default_cluster_name: name of cluster to set default values for.
            yaml_file: the yaml file that is providing the default compute resource values.
            set_task_templates: whether or not the user wants to set the default compute
                resource values for all of the TaskTemplates associated with Tool.
            ignore_missing_keys: Whether or not to raise an error if a key is missing from the
                yaml file.
        """
        with open(yaml_file, "r") as stream:
            try:
                default_compute_resources = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise ValueError(
                    f"Unable to read default compute resources from " f"{yaml_file}."
                ) from exc

        # Set the Tool level compute resources
        try:
            compute_resources = default_compute_resources["tool_resources"][
                default_cluster_name
            ]
        except KeyError as exc:
            msg = f"No Tool resources matching cluster name in yaml file: {yaml_file}."
            if ignore_missing_keys:
                logger.info(msg)
            else:
                raise KeyError(msg) from exc

        self.active_tool_version.set_default_compute_resources_from_dict(
            cluster_name=default_cluster_name, compute_resources=compute_resources
        )
        self.active_tool_version.default_cluster_name = default_cluster_name

        if not set_task_templates:
            return

        if not self.active_task_templates:
            raise Exception(
                "No TaskTemplates associated with Tool, unable to set default "
                "compute resources for TaskTemplates."
            )

        if set_task_templates:
            # Set the the compute resources for the TaskTemplates associated with the Tool
            for tt in self.active_task_templates.values():
                try:
                    tt.set_default_compute_resources_from_dict(
                        cluster_name=default_cluster_name,
                        compute_resources=(
                            default_compute_resources["task_template_resources"][
                                tt.template_name
                            ][default_cluster_name]
                        ),
                    )
                except KeyError as exc:
                    msg = (
                        f"No compute resources discovered in yaml file {yaml_file} for"
                        f"TaskTemplate {tt.template_name}"
                    )
                    if ignore_missing_keys:
                        logger.info(msg)
                    else:
                        raise KeyError(msg) from exc

    def set_default_compute_resources_from_dict(
        self, cluster_name: str, compute_resources: Dict[str, Any]
    ) -> None:
        """Set default compute resources for a given cluster_name.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to set default values for.
            compute_resources: dictionary of default compute resources to run tasks
                with. Can be overridden at task template or task level.
                dict of {resource_name: resource_value}
        """
        if not self.default_cluster_name:
            self.active_tool_version.default_cluster_name = cluster_name
        self.active_tool_version.set_default_compute_resources_from_dict(
            cluster_name, compute_resources
        )

    def set_default_cluster_name(self, cluster_name: str) -> None:
        """Set default cluster.

        Args:
            cluster_name: name of cluster to set as default.
        """
        self.active_tool_version.default_cluster_name = cluster_name

    def _load_tool_versions(self) -> List[ToolVersion]:
        app_route = f"/tool/{self.id}/tool_versions"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )

        tool_versions = [
            ToolVersion.from_wire(wire_tuple)
            for wire_tuple in response["tool_versions"]
        ]
        return tool_versions

    def _bind(self) -> None:
        """Call route to create tool."""
        app_route = "/tool"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"name": self.name},
            request_type="post",
            logger=logger,
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        self.id = SerializeClientTool.kwargs_from_wire(response["tool"])["id"]

    def __repr__(self) -> str:
        """A representation string for a Tool instance."""
        return f"Tool(tool_id={self.id}, name={self.name}"
