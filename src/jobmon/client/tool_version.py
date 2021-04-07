from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Dict, List, Optional, Tuple

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template import TaskTemplate
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientToolVersion


import structlog as logging


logger = logging.getLogger(__name__)


class ToolVersion:

    def __init__(self, tool_version_id: int, requester: Optional[Requester] = None) -> None:
        self.id = tool_version_id
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.task_templates: Dict[str, TaskTemplate] = {}

    @classmethod
    def get_tool_version(cls, tool_id, requester: Optional[Requester] = None) -> ToolVersion:
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)

        app_route = "/client/tool_version"
        return_code, response = requester.send_request(
            app_route=app_route,
            message={"tool_id": tool_id},
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
        tool_version_kwargs = SerializeClientToolVersion.kwargs_from_wire(wire_tuple)
        tool_version = cls(tool_version_kwargs["id"], requester=requester)
        return tool_version

    def load_task_templates(self):
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
        task_template = self.task_templates.get(template_name)
        if task_template is None:
            task_template = TaskTemplate.get_task_template(self.id, template_name,
                                                           requester=self.requester)
            task_template.load_task_template_versions()
            self.task_templates[template_name] = task_template
        return task_template
