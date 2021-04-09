"""Tool Version represents a logical instance of a project or model that will be run many times
over.
"""
from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Dict, Optional, Tuple

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template import TaskTemplate
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientToolVersion


import structlog as logging


logger = logging.getLogger(__name__)


class ToolVersion:
    """Tool Version represents a logical instance of a project or model that will be run many times
    over.
    """

    def __init__(self, tool_version_id: int, requester: Optional[Requester] = None) -> None:
        """Instantiate a tool version.

        Args:
            tool_version_id: an integer id associated with a Tool
            requester: communicate with the flask services.
        """
        self.id = tool_version_id
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.task_templates: Dict[str, TaskTemplate] = {}

    @classmethod
    def get_tool_version(cls, tool_version_id, requester: Optional[Requester] = None
                         ) -> ToolVersion:
        """Get an instance of ToolVersion from the database.

        Args:
            tool_version_id: an integer id associated with a Tool
            requester: communicate with the flask services.
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)

        app_route = "/client/tool_version"
        return_code, response = requester.send_request(
            app_route=app_route,
            message={"tool_version_id": tool_version_id},
            request_type='post',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )
        tool_version = cls.from_wire(response["tool_version"], requester)
        return tool_version

    @classmethod
    def from_wire(cls, wire_tuple: Tuple, requester: Optional[Requester] = None
                  ) -> ToolVersion:
        """Convert from the wire format of ToolVersion to an instance

        Args:
            wire_tuple: Wire format for ToolVersion defined in jobmon.serializers.
            requester: communicate with the flask services.
        """
        tool_version_kwargs = SerializeClientToolVersion.kwargs_from_wire(wire_tuple)
        tool_version = cls(tool_version_kwargs["id"], requester=requester)
        return tool_version

    def load_task_templates(self):
        """Get all task_templates associated with this tool version from the database."""
        app_route = f'/tool_version/{self.id}/task_templates'
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
        task_templates = [TaskTemplate.from_wire(wire_tuple, requester=self.requester)
                          for wire_tuple in response["task_templates"]]
        for task_template in task_templates:
            self.task_templates[task_template.template_name] = task_template
            task_template.load_task_template_versions()

    def get_task_template(self, template_name: str) -> TaskTemplate:
        """Get a single task_template associated with this tool version from the database."""
        task_template = self.task_templates.get(template_name)
        if task_template is None:
            task_template = TaskTemplate.get_task_template(self.id, template_name,
                                                           requester=self.requester)
            task_template.load_task_template_versions()
            self.task_templates[template_name] = task_template
        return task_template
