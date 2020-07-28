from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Optional, List, Callable, Union

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.task import Task
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.client.requests.requester import Requester
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.exceptions import InvalidResponse


logger = logging.getLogger(__name__)


class TaskTemplate:

    def __init__(self, tool_version_id: int, template_name: str,
                 requester: Requester = shared_requester) -> None:
        """Groups tasks of a type, by declaring the concrete arguments that instances may vary
        over either from workflow to workflow or between nodes in the stage of a dag.

        Args:
            tool_version_id: the version of the tool this task template is associated with.
            template_name: the name of this task template.
            requester: requester for communicating with central services
        """

        # add requester for url
        self.requester = requester

        # task template keys
        self.tool_version_id = tool_version_id
        self.template_name = template_name

    @property
    def task_template_id(self) -> int:
        if not hasattr(self, "_task_template_id"):
            raise AttributeError("Cannot access task_template_id until TaskTemplate is bound")
        return self._task_template_id

    @property
    def task_template_version(self) -> TaskTemplateVersion:
        if not hasattr(self, "_task_template_version"):
            raise AttributeError(
                "Cannot access task_template_version until TaskTemplateVersion is bound")
        return self._task_template_version

    def bind(self):
        task_template_id = self._get_task_template_id()
        if task_template_id is None:
            task_template_id = self._insert_task_template()
        self._task_template_id = task_template_id

    def bind_task_template_version(self, command_template: str, node_args: List[str] = [],
                                   task_args: List[str] = [], op_args: List[str] = []):
        """Bind a task template version instance to the db.

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
        # if we have argument overlap
        if "name" in self.task_template_version.template_args:
            kwargs["name"] = name

        # kwargs quality assurance
        if self.task_template_version.template_args != set(kwargs.keys()):
            raise ValueError(
                f"unexpected kwargs. expected {self.task_template_version.template_args} -"
                f"received {set(kwargs.keys())}")

        command = self.task_template_version.command_template.format(**kwargs)

        # arg id name mappings
        node_args = {self.task_template_version.id_name_map[k]: v
                     for k, v in kwargs.items() if k in self.task_template_version.node_args}
        task_args = {self.task_template_version.id_name_map[k]: v
                     for k, v in kwargs.items() if k in self.task_template_version.task_args}

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
            task_attributes=task_attributes)
        return task

    def _get_task_template_id(self) -> Optional[int]:
        app_route = "/task_template"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tool_version_id": self.tool_version_id,
                     "task_template_name": self.template_name},
            request_type='get'
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_id"]

    def _insert_task_template(self) -> int:
        app_route = "/task_template"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"tool_version_id": self.tool_version_id,
                     "task_template_name": self.template_name},
            request_type='post'
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_id"]
