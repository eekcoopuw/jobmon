from typing import List, Union

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.workflow.task_template import TaskTemplate
# from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.serializers import SerializeClientTool, SerializeClientToolVersion


class InvalidToolError(Exception):
    pass


class InvalidToolVersionError(Exception):
    pass


class Tool:

    def __init__(self, name: str = "unknown",
                 active_tool_version_id: Union[str, int] = "latest",
                 requester: Requester = shared_requester) -> None:
        """A tool is an application with is expected to run many times on
        variable inputs.

        Args:
            name: the name of the tool
            active_tool_version_id: which version of the tool to attach task
                templates and workflows to
            requester: requester object directed at a flask service instance
        """
        self.requester = requester
        self.name = name
        self.id = self._get_tool_id(self.name, self.requester)

        # which tool version are they using for a run
        self.tool_version_ids = sorted(self._get_tool_version_ids())
        self.active_tool_version_id = active_tool_version_id

    @classmethod
    def create_tool(cls, name: str, requester: Requester = shared_requester
                    ) -> "Tool":
        """create a new tool in the jobmon database

        Args:
            name: the name of the tool
            requester: requester object directed at a flask service instance

        Returns:
            An instance of of Tool of with the provided name
        """

        # call route to create tool
        _, res = requester.send_request(
            app_route="/tool",
            message={"name": name},
            request_type='post')
        tool_id = SerializeClientTool.kwargs_from_wire(res["tool"])["id"]

        # also create a new version
        cls._create_new_tool_version(tool_id, requester)

        # return instance of new tool
        return cls(name)

    def create_new_tool_version(self) -> int:
        """create a new tool version for the current tool and activate it

        Returns: the version id for the new tool
        """
        # call route to create tool version
        tool_version_id = self._create_new_tool_version(self.id,
                                                        self.requester)
        self.tool_version_ids.append(tool_version_id)
        self.active_tool_version_id = tool_version_id
        return tool_version_id

    @property
    def active_tool_version_id(self):
        return self._active_tool_version_id

    @active_tool_version_id.setter
    def active_tool_version_id(self, val):
        if val == "latest":
            tool_version_id: int = self.tool_version_ids[-1]
        else:
            if val not in self.tool_version_ids:
                raise InvalidToolVersionError(
                    f"tool_version_id {val} is not a valid tool version for "
                    f"{self.name}. Valid tool versions are "
                    f"{self.tool_version_ids}")
            tool_version_id: int = val
        self._active_tool_version_id = tool_version_id

    def get_task_type(self, template_name, command_template: str,
                      node_args: List[str], data_args: List[str],
                      op_args: List[str]) -> TaskTemplate:
        tt = TaskTemplate(self.name, template_name, command_template,
                          node_args, data_args, op_args)
        return tt

    def _get_tool_version_ids(self) -> List[int]:
        app_route = f"/tool/{self.id}/tool_versions"
        _, res = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        tool_versions = [
            SerializeClientToolVersion.kwargs_from_wire(wire_tuple)["id"]
            for wire_tuple in res["tool_versions"]]
        return tool_versions

    @staticmethod
    def _get_tool_id(name: str, requester: Requester) -> int:
        app_route = f"/tool/{name}"
        _, res = requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if res["tool"] is None:
            raise InvalidToolError(
                f"no tool found in database for name: {name}. Use create_tool"
                "to make a new tool.")
        else:
            return SerializeClientTool.kwargs_from_wire(res["tool"])["id"]

    @staticmethod
    def _create_new_tool_version(tool_id: int, requester: Requester) -> int:
        app_route = "/tool_version"
        _, res = requester.send_request(
            app_route=app_route,
            message={"tool_id": tool_id},
            request_type='post')
        tool_version = SerializeClientToolVersion.kwargs_from_wire(
            res["tool_version"])
        return tool_version["id"]
