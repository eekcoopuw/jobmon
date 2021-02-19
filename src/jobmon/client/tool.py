"""Tool represents a project or model that will be run many times over, but may evolve over
time.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template import TaskTemplate
from jobmon.client.workflow import Workflow
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientTool, SerializeClientToolVersion

import structlog as logging

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

    def __init__(self, name: str = "unknown",
                 active_tool_version_id: Union[str, int] = "latest",
                 requester: Optional[Requester] = None) -> None:
        """A tool is an application which is expected to run many times on variable inputs but
         which will serve a certain purpose over time even as the internal pipeline may change.
         Example tools are Dismod, Burdenator, Codem.

        Args:
            name: the name of the tool
            active_tool_version_id: which version of the tool to attach task templates and
                workflows to.
            requester_url (str): url to communicate with the flask services.
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.name = name
        self.id = self._get_tool_id(self.name, self.requester)
        self.task_templates: Dict[int, TaskTemplate] = {}

        # which tool version are they using for a run
        self.tool_version_ids = sorted(self._get_tool_version_ids())
        self.active_tool_version_id = active_tool_version_id

    @classmethod
    def create_tool(cls, name: str, requester: Optional[Requester] = None) -> Tool:
        """Create a new tool in the jobmon database.

        Args:
            name: the name of the tool
            requester: requester object directed at a flask service instance

        Returns:
            An instance of of Tool of with the provided name
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)

        # call route to create tool
        _, res = requester.send_request(
            app_route="/client/tool",
            message={"name": name},
            request_type='post',
            logger=logger
        )

        if res["tool"] is not None:
            tool_id = SerializeClientTool.kwargs_from_wire(res["tool"])["id"]
            # also create a new version
            cls._create_new_tool_version(tool_id, requester)

        # return instance of new tool
        return cls(name)

    def create_new_tool_version(self) -> int:
        """Create a new tool version for the current tool and activate it.

        Returns: the version id for the new tool
        """
        # call route to create tool version
        tool_version_id = self._create_new_tool_version(self.id, self.requester)
        self.tool_version_ids.append(tool_version_id)
        self.active_tool_version_id = tool_version_id
        return tool_version_id

    @property
    def active_tool_version_id(self) -> int:
        """Tool version id to use when spawning task templates."""
        return self._active_tool_version_id

    @active_tool_version_id.setter
    def active_tool_version_id(self, val: Union[str, int]):
        """Tool version that is set as the active one (latest is default)."""
        if val == "latest":
            self._active_tool_version_id = self.tool_version_ids[-1]
        else:
            if val not in self.tool_version_ids:
                raise InvalidToolVersionError(
                    f"tool_version_id {val} is not a valid tool version for {self.name}. Valid"
                    f" tool versions are {self.tool_version_ids}")
            self._active_tool_version_id = int(val)

    def get_task_template(self, template_name: str, command_template: str,
                          node_args: List[str] = [], task_args: List[str] = [],
                          op_args: List[str] = []) -> TaskTemplate:
        """Create or get task a task template

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
        tt = TaskTemplate(self.active_tool_version_id, template_name, self.requester)
        if hash(tt) in self.task_templates.keys():
            task_template_id = self.task_templates[hash(tt)].task_template_id
            tt.bind(task_template_id)
        else:
            self.task_templates[hash(tt)] = tt
            tt.bind()
        tt.bind_task_template_version(command_template=command_template,
                                      node_args=node_args,
                                      task_args=task_args,
                                      op_args=op_args)
        return tt

    def create_workflow(self, workflow_args: str = "", name: str = "", description: str = "",
                        workflow_attributes: Optional[Union[List, dict]] = None,
                        max_concurrently_running: int = 10_000,
                        requester_url: Optional[str] = None, chunk_size: int = 500) \
            -> Workflow:
        """Create a workflow object associated with the tool."""
        if requester_url is None:
            requester_url = self.requester.url
        wf = Workflow(self.active_tool_version_id, workflow_args, name, description,
                      workflow_attributes, max_concurrently_running, requester=self.requester,
                      chunk_size=chunk_size)
        return wf

    def _get_tool_version_ids(self) -> List[int]:
        app_route = f"/client/tool/{self.id}/tool_versions"
        _, res = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get',
            logger=logger
        )
        tool_versions = [
            SerializeClientToolVersion.kwargs_from_wire(wire_tuple)["id"]
            for wire_tuple in res["tool_versions"]
        ]
        return tool_versions

    @staticmethod
    def _get_tool_id(name: str, requester: Requester) -> int:
        app_route = f"/client/tool/{name}"
        _, res = requester.send_request(
            app_route=app_route,
            message={},
            request_type='get',
            logger=logger
        )
        if res["tool"] is None:
            raise InvalidToolError(
                f"no tool found in database for name: {name}. Use create_tool to make a new "
                f"tool.")
        else:
            return SerializeClientTool.kwargs_from_wire(res["tool"])["id"]

    @staticmethod
    def _create_new_tool_version(tool_id: int, requester: Requester) -> int:
        app_route = "/client/tool_version"
        _, res = requester.send_request(
            app_route=app_route,
            message={"tool_id": tool_id},
            request_type='post',
            logger=logger
        )
        tool_version = SerializeClientToolVersion.kwargs_from_wire(res["tool_version"])
        return tool_version["id"]
