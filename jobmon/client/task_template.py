import hashlib
from string import Formatter
from typing import Optional, List, Callable, Union

from jobmon.serializers import SerializeClientTaskTemplateVersion
from jobmon.client import shared_requester
from jobmon.client.task import Task
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm.executors.base import ExecutorParameters


class TaskTemplateVersion:

    def __init__(self, task_template_version_id, id_name_map):
        self.id = task_template_version_id
        self.id_name_map = id_name_map

    @classmethod
    def from_wire(cls, wire_tuple: tuple) -> "TaskTemplateVersion":
        kwargs = SerializeClientTaskTemplateVersion.kwargs_from_wire(
            wire_tuple)
        return cls(**kwargs)


class TaskTemplate:

    def __init__(self, tool_version_id: int, template_name: str,
                 command_template: str, node_args: List[str] = [],
                 task_args: List[str] = [], op_args: List[str] = [],
                 requester: Requester = shared_requester) -> None:
        """Groups tasks of a type, by declaring the concrete arguments that
        instances may vary over

        Args:
            tool_version_id: the version of the tool this task template is
                associated with.
            template_name: the name of this task template.
            command_template: an abstract command representing a task, where
                the arguments to the command have defined names but the values
                are not assigned. eg:
                    '{python} {script} --data {data} --para {para} {verbose}'
            node_args: any named arguments in command_template that make the
                command unique within this template for a given workflow run.
                Generally these are arguments that can be parallelized over.
            task_args: any named arguments in command_template that make the
                command unique across workflows if the node args are the same
                as a previous workflow. Generally these are arguments about
                data moving though the task.
            op_args: any named arguments in command_template that can change
                without changing the identity of the task. Generally these
                are things like the task executable location or the verbosity
                of the script.
            requester: requester for communicating with central services
        """

        # add requester for url
        self.requester = requester

        # task template keys
        self.tool_version_id = tool_version_id
        self.template_name = template_name
        self.task_template_id = self._get_task_template_id()

        # task template version arguments
        self._template_version_created = False
        self.command_template = command_template
        self.node_args = node_args
        self.task_args = task_args
        self.op_args = op_args
        self.task_template_version = self._get_task_template_version()

    @property
    def task_template_version_id(self):
        return self.task_template_version.id

    @property
    def arg_id_name_map(self):
        """The mapping between argument names and the ids in the database"""
        return self.task_template_version.id_name_map

    @property
    def template_args(self):
        """The argument names in the command template"""
        return set([i[1] for i in Formatter().parse(self.command_template)
                    if i[1] is not None])

    @property
    def node_args(self):
        """any named arguments in command_template that make the command unique
        within this template for a given workflow run. Generally these are
        arguments that can be parallelized over."""
        return self._node_args

    @node_args.setter
    def node_args(self, val):
        if self._template_version_created:
            raise AttributeError(
                "Cannot set node_args. node_args must be declared during "
                "instantiation")
        val = set(val)
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must be as a "
                "superset of the keys declared in node_args. Values recieved "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\nnode_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}.")
        self._node_args = val

    @property
    def task_args(self):
        """any named arguments in command_template that make the command unique
        across workflows if the node args are the same as a previous workflow.
        Generally these are arguments about data moving though the task."""
        return self._task_args

    @task_args.setter
    def task_args(self, val):
        if self._template_version_created:
            raise AttributeError(
                "Cannot set task_args. task_args must be declared during "
                "instantiation")
        val = set(val)
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must be as a "
                "superset of the keys declared in task_args. Values recieved "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\nnode_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}.")
        self._task_args = val

    @property
    def op_args(self):
        """any named arguments in command_template that can change without
        changing the identity of the task. Generally these are things like the
        task executable location or the verbosity of the script."""
        return self._op_args

    @op_args.setter
    def op_args(self, val):
        if self._template_version_created:
            raise AttributeError(
                "Cannot set op_args. op_args must be declared during "
                "instantiation")
        val = set(val)
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must be as a "
                "superset of the keys declared in op_args. Values recieved "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\nnode_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}.")
        self._op_args = val

    def create_task(self,
                    executor_parameters: Union[ExecutorParameters, Callable],
                    name: Optional[str] = None,
                    upstream_tasks: List["Task"] = [],
                    max_attempts: Optional[int] = 3,
                    job_attributes: Optional[dict] = None,
                    **kwargs) -> Task:
        """Create an instance of a task associated with this template

        Args:
            executor_parameters: an instance of executor paremeters class
            name: a name associated with this specific task
            upstream_task: Task objects that must be run prior to this one
            max_attempts: Number of attempts to try this task before giving up.
                Default is 3.
            job_attributes: any attributes associated with this task to be
                tracked.
            **kwargs: values for each argument specified in command_template

        Returns: ExecutableTask
        """
        if self.template_args != set(kwargs.keys()):
            raise ValueError(
                f"unexpected kwargs. expected {self.template_args} -"
                f"recieved {set(kwargs.keys())}")

        command = self.command_template.format(**kwargs)

        # arg id name mappings
        node_args = {self.arg_id_name_map[k]: v
                     for k, v in kwargs.items() if k in self.node_args}
        task_args = {self.arg_id_name_map[k]: v
                     for k, v in kwargs.items() if k in self.task_args}

        # build task
        task = Task(
            command=command,
            task_template_version_id=self.task_template_version_id,
            node_args=node_args,
            task_args=task_args,
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            job_attributes=job_attributes)
        return task

    def _get_task_template_id(self) -> int:
        _, res = self.requester.send_request(
            app_route=f"/task_template",
            message={"tool_version_id": self.tool_version_id,
                     "task_template_name": self.template_name},
            request_type='get')

        if res["task_template_id"] is not None:
            task_template_id = res["task_template_id"]
        else:
            _, res = self.requester.send_request(
                app_route=f"/task_template",
                message={"tool_version_id": self.tool_version_id,
                         "name": self.template_name},
                request_type='post')
            task_template_id = res["task_template_id"]
        return task_template_id

    def _get_task_template_version(self) -> TaskTemplateVersion:
        hashable = "".join(sorted(self._node_args) +
                           sorted(self._task_args) +
                           sorted(self._op_args))
        arg_mapping_hash = int(
            hashlib.sha1(hashable.encode('utf-8')).hexdigest(), 16)

        _, res = self.requester.send_request(
            app_route=f"/task_template/{self.task_template_id}/version",
            message={
                "command_template": self.command_template,
                "arg_mapping_hash": arg_mapping_hash,
            },
            request_type="get")

        if res["task_template_version"] is not None:
            wire_args = res["task_template_version"]
        else:
            app_route = f"/task_template/{self.task_template_id}/add_version"
            _, res = self.requester.send_request(
                app_route=app_route,
                message={"command_template": self.command_template,
                         "arg_mapping_hash": arg_mapping_hash,
                         "node_args": list(self._node_args),
                         "task_args": list(self._task_args),
                         "op_args": list(self._op_args)},
                request_type='post')
            wire_args = res["task_template_version"]

        ttv = TaskTemplateVersion.from_wire(wire_args)
        self._template_version_created = True

        return ttv
