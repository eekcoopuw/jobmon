"""A Task Template defines a framework that many tasks have in common while varying by a
declared set of arguments.
"""
from __future__ import annotations

import hashlib
import logging
from http import HTTPStatus as StatusCodes
from typing import Any, Callable, List, Optional, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.client.task import Task
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
from jobmon.serializers import SerializeTaskTemplateResourceUsage

logger = logging.getLogger(__name__)


class TaskTemplate:
    """A Task Template defines a framework that many tasks have in common while varying by a
    declared set of arguments.
    """

    def __init__(self, tool_version_id: int, template_name: str,
                 requester: Optional[Requester] = None) -> None:
        """Groups tasks of a type, by declaring the concrete arguments that instances may vary
        over either from workflow to workflow or between nodes in the stage of a dag.

        Args:
            tool_version_id: the version of the tool this task template is associated with.
            template_name: the name of this task template.
            requester_url (str): url to communicate with the flask services.
        """
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # task template keys
        self.tool_version_id = tool_version_id
        self.template_name = template_name

        self._task_template_id = None

    @property
    def task_template_id(self) -> int:
        """Unique id from db if task_template has been bound."""
        if not hasattr(self, "_task_template_id"):
            raise AttributeError("Cannot access task_template_id until TaskTemplate is bound")
        return self._task_template_id

    @property
    def task_template_version(self) -> TaskTemplateVersion:
        """Version of task template if it has been bound."""
        if not hasattr(self, "_task_template_version"):
            raise AttributeError(
                "Cannot access task_template_version until TaskTemplateVersion is bound")
        return self._task_template_version

    def bind(self, task_template_id=None):
        """Bind task template to the db."""
        if task_template_id is None:
            task_template_id = self._get_task_template_id()
            if task_template_id is None:
                task_template_id = self._insert_task_template()
        self._task_template_id = task_template_id

    def bind_task_template_version(self, command_template: str, node_args: List[str] = [],
                                   task_args: List[str] = [], op_args: List[str] = []):
        """Bind a task template version instance to the db.

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
        """
        if not node_args:
            node_args = []
        if not task_args:
            task_args = []
        if not op_args:
            op_args = []

        task_template_version = TaskTemplateVersion(
            task_template_id=self.task_template_id,
            command_template=command_template,
            node_args=node_args,
            task_args=task_args,
            op_args=op_args,
            requester=self.requester
        )
        task_template_version.bind()
        self._task_template_version = task_template_version

    def create_task(self,
                    executor_parameters: Union[ExecutorParameters, Callable],
                    name: Optional[str] = None,
                    upstream_tasks: List[Task] = [],
                    task_attributes: Union[List, dict] = {},
                    max_attempts: int = 3,
                    **kwargs) -> Task:
        """Create an instance of a task associated with this template.

        Args:
            executor_parameters: an instance of executor paremeters class
            name: a name associated with this specific task
            upstream_tasks: Task objects that must be run prior to this one
            task_attributes (dict or list): attributes and their values or just the attributes
                that will be given values later
            max_attempts: Number of attempts to try this task before giving up. Default is 3.
            **kwargs: values for each argument specified in command_template

        Returns:
            ExecutableTask

        Raises:
            ValueError: if the args that are supplied do not match the args in the command
                template.
        """
        # arg id name mappings
        node_args = {self.task_template_version.id_name_map[k]: str(v)
                     for k, v in kwargs.items() if k in self.task_template_version.node_args}
        task_args = {self.task_template_version.id_name_map[k]: str(v)
                     for k, v in kwargs.items() if k in self.task_template_version.task_args}

        # use a default name when not provided
        if name is None:
            name = self.template_name + "_" + \
                '_'.join([str(k) + "-" + str(node_args[k]) for k in node_args.keys()])
            # long name protection
            name = name if len(name) < 256 else name[0:254]

        # if we have argument overlap
        if "name" in self.task_template_version.template_args:
            kwargs["name"] = name

        # kwargs quality assurance
        if self.task_template_version.template_args != set(kwargs.keys()):
            raise ValueError(
                f"unexpected kwargs. expected {self.task_template_version.template_args} -"
                f"received {set(kwargs.keys())}")

        command = self.task_template_version.command_template.format(**kwargs)

        # build task
        task = Task(
            command=command,
            task_template_version_id=self.task_template_version.id,
            node_args=node_args,
            task_args=task_args,
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes,
            requester=self.requester
        )
        return task

    def _get_task_template_id(self) -> Optional[int]:
        app_route = "/client/task_template"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tool_version_id": self.tool_version_id,
                     "task_template_name": self.template_name},
            request_type='get',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_id"]

    def _insert_task_template(self) -> int:
        app_route = "/client/task_template"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tool_version_id": self.tool_version_id,
                     "task_template_name": self.template_name},
            request_type='post',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_id"]

    def __hash__(self):
        """A task_template_hash is a hash of the TaskTemplate name and tool version
        concatenated together.
        """
        hash_value = int(hashlib.sha1(
            ''.join(self.template_name + str(self.tool_version_id)).encode(
                'utf-8')).hexdigest(), 16)
        return hash_value

    def resource_usage(self, workflows: List[int] = None,
                       node_args: dict[str, Any] = None,
                       ci: float = None) -> dict:
        """Get the aggregate resource usage for a TaskTemplate."""
        message = {'task_template_version_id': self.task_template_version.id}
        if workflows:
            message['workflows'] = workflows
        if node_args:
            message["node_args"] = node_args
        if ci:
            message["ci"] = ci
        app_route = "/client/task_template_resource_usage"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type='post',
            logger=logger
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}'
            )

        def format_bytes(value):
            if value is not None:
                return str(value) + "B"
            else:
                return value

        kwargs = SerializeTaskTemplateResourceUsage.kwargs_from_wire(response)
        resources = {'num_tasks': kwargs["num_tasks"],
                     'min_mem': format_bytes(kwargs["min_mem"]),
                     'max_mem': format_bytes(kwargs["max_mem"]),
                     'mean_mem': format_bytes(kwargs["mean_mem"]),
                     'min_runtime': kwargs["min_runtime"],
                     'max_runtime': kwargs["max_runtime"],
                     'mean_runtime': kwargs["mean_runtime"],
                     "median_mem": format_bytes(kwargs['median_mem']),
                     "median_runtime": kwargs['median_runtime'],
                     'ci_mem': kwargs['ci_mem'],
                     'ci_runtime': kwargs['ci_runtime']}
        return resources
