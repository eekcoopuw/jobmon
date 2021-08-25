"""A logical instance of a project or model that will be run many times over."""
from __future__ import annotations

from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Dict, Optional, Tuple

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template import TaskTemplate
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeClientToolVersion


logger = logging.getLogger(__name__)


class ToolVersion:
    """Represents a logical instance of a project or model that will be run many times over."""

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

        self.default_compute_resources_set: Dict[str, Dict[str, Any]] = {}
        self.default_cluster_name: str = ""

    @classmethod
    def get_tool_version(cls: Any, tool_id: Optional[int] = None,
                         tool_version_id: Optional[int] = None,
                         requester: Optional[Requester] = None) -> ToolVersion:
        """Get an instance of ToolVersion from the database.

        Args:
            tool_id: an integer id associated with a Tool
            tool_version_id: tool_version_id to get from the database.
            requester: communicate with the flask services.
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)

        message = {}
        if tool_id is not None:
            message["tool_id"] = tool_id
        if tool_version_id is not None:
            message["tool_version_id"] = tool_version_id
        if not message:
            raise ValueError(
                "get_tool_version must specify either a tool_id or a tool_version_id"
            )

        app_route = "/client/tool_version"
        return_code, response = requester.send_request(
            app_route=app_route,
            message=message,
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
    def from_wire(cls: Any, wire_tuple: Tuple, requester: Optional[Requester] = None
                  ) -> ToolVersion:
        """Convert from the wire format of ToolVersion to an instance.

        Args:
            wire_tuple: Wire format for ToolVersion defined in jobmon.serializers.
            requester: communicate with the flask services.
        """
        tool_version_kwargs = SerializeClientToolVersion.kwargs_from_wire(wire_tuple)
        tool_version = cls(tool_version_kwargs["id"], requester=requester)
        return tool_version

    def load_task_templates(self) -> None:
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

    def update_default_compute_resources(self, cluster_name: str, **kwargs: Any) -> None:
        """Update default compute resources in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        compute_resources = {cluster_name: kwargs}
        self.default_compute_resources_set.update(compute_resources)

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
        self.default_compute_resources_set[cluster_name] = compute_resources
