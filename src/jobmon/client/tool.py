"""Tool represents a project or model that will be run many times over, but may evolve over
time.
"""
from __future__ import annotations

import getpass
from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Dict, List, Optional, Union
import warnings

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
    """Tool represents a project or model that will be run many times over, but may evolve over
    time.
    """

    def __init__(self, name: str = f"unknown-{getpass.getuser()}",
                 active_tool_version_id: Union[str, int] = "latest",
                 requester: Optional[Requester] = None) -> None:
        """A tool is an application which is expected to run many times on variable inputs but
         which will serve a certain purpose over time even as the internal pipeline may change.
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
        self._load_tool_versions()
        if not self.tool_versions:
            self.get_new_tool_version()
        else:
            self.set_active_tool_version_id(active_tool_version_id)

    def get_new_tool_version(self) -> int:
        """Create a new tool version for the current tool and activate it.

        Returns: the version id for the new tool
        """
        # call route to create tool version

        tool_version = ToolVersion.get_tool_version(tool_id=self.id, requester=self.requester)
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
    def default_compute_resources_set(self):
        return self.active_tool_version.default_compute_resources_set

    @property
    def default_cluster_name(self):
        return self.active_tool_version.cluster_name

    def set_active_tool_version_id(self, tool_version_id: Union[str, int]):
        """Tool version that is set as the active one (latest is default during instantiation).

        Args:
            tool_version_id: which tool version to set as active on this object.
        """
        version_index_lookup = {self.tool_versions[index].id: index
                                for index in range(len(self.tool_versions))}

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

    def create_task_template(self, template_name: str, command_template: str,
                             node_args: List[str] = [], task_args: List[str] = [],
                             op_args: List[str] = []) -> TaskTemplate:
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
        """
        warnings.warn(
            "The create_task_template method is deprecated. Please use get_task_template.",
            DeprecationWarning
        )
        return self.get_task_template(template_name, command_template, node_args, task_args,
                                      op_args)

    def get_task_template(self, template_name: str, command_template: str,
                          node_args: List[str] = [], task_args: List[str] = [],
                          op_args: List[str] = []) -> TaskTemplate:
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
        """
        tt = self.active_tool_version.get_task_template(template_name)
        tt.get_task_template_version(command_template, node_args, task_args, op_args)
        return tt

    def create_workflow(self, workflow_args: str = "", name: str = "", description: str = "",
                        workflow_attributes: Optional[Union[List, dict]] = None,
                        max_concurrently_running: int = 10_000, chunk_size: int = 500
                        ) -> Workflow:
        """Create a workflow object associated with the tool."""
        wf = Workflow(self.active_tool_version.id, workflow_args, name, description,
                      workflow_attributes, max_concurrently_running, requester=self.requester,
                      chunk_size=chunk_size)
        if self.default_cluster_name:
            wf.default_cluster_name = self.default_cluster_name
        if self.active_tool_version.default_compute_resources_set:
            wf.default_compute_resources_set = self.default_compute_resources_set
        return wf

    def update_default_compute_resources(self, cluster_name: str, **kwargs):
        self.active_tool_version.update_default_compute_resources(cluster_name, **kwargs)

    def set_default_compute_resources_from_yaml(self, cluster_name: str, yaml_file: str):
        pass

    def set_default_compute_resources_from_dict(self, cluster_name: str,
                                                dictionary: Dict[str, Any]):
        self.active_tool_version.set_default_compute_resources_from_dict(cluster_name,
                                                                         dictionary)

    def set_default_cluster_name(self, cluster_name: str):
        self.active_tool_version.default_cluster_name = cluster_name

    def _load_tool_versions(self):
        app_route = f"/client/tool/{self.id}/tool_versions"
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

        tool_versions = [ToolVersion.from_wire(wire_tuple)
                         for wire_tuple in response["tool_versions"]]
        self.tool_versions = tool_versions

    def _bind(self):
        # call route to create tool
        app_route = "/client/tool"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"name": self.name},
            request_type='post',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )
        self.id = SerializeClientTool.kwargs_from_wire(response["tool"])["id"]
