from string import Formatter
from typing import Optional, List, Callable, Union

from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.executable_task import ExecutableTask


class TaskTemplate:

    def __init__(self, tool: str, template_name: str, command_template: str,
                 node_args: List[str] = [], data_args: List[str] = [],
                 op_args: List[str] = []):

        # task template keys
        self.tool = tool
        self.template_name = template_name

        # command strucuture
        self.command_template = command_template
        self.node_args = node_args
        self.data_args = data_args
        self.op_args = op_args

    @property
    def template_key_set(self):
        return set(
            [i[1] for i in Formatter().parse(self.command_template)
             if i[1] is not None])

    @property
    def node_args(self):
        return self._node_args

    @node_args.setter
    def node_args(self, val):
        val = set(val)
        if not self.template_key_set.issuperset(val):
            raise ValueError(
                "template_key_set must be a superset of node_args")
        self._node_args = val

    @property
    def data_args(self):
        return self._data_args

    @data_args.setter
    def data_args(self, val):
        val = set(val)
        if not self.template_key_set.issuperset(val):
            raise ValueError(
                "template_key_set must be a superset of data_args")
        self._data_args = val

    @property
    def op_args(self):
        return self._op_args

    @op_args.setter
    def op_args(self, val):
        val = set(val)
        if not self.template_key_set.issuperset(val):
            raise ValueError("template_key_set must be a superset of op_args")
        self._op_args = val

    def get_task(self,
                 executor_parameters: Union[ExecutorParameters, Callable],
                 name: Optional[str] = None,
                 upstream_tasks: Optional[List["ExecutableTask"]] = None,
                 max_attempts: Optional[int] = 3,
                 job_attributes: Optional[dict] = None,
                 **kwargs):

        if self.template_key_set != set(kwargs.keys()):
            raise ValueError(
                f"unexpected kwargs. expected {self.template_key_set} -"
                f"recieved {set(kwargs.keys())}")

        command = self.command_template.format(**kwargs)

        # construct node here
        node_arg_vals = {k: v for k, v in kwargs.items()
                         if k in self.node_args}

        # build task
        data_arg_vals = {k: v for k, v in kwargs.items()
                         if k in self.data_args}
        task = ExecutableTask(
            command=command,
            node_arg_vals=node_arg_vals,
            data_arg_vals=data_arg_vals,
            executor_parameters=executor_parameters,
            name=name,
            upstream_tasks=upstream_tasks,
            max_attempts=max_attempts,
            job_attributes=job_attributes)
        return task
