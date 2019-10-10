from string import Formatter
from typing import List

from jobmon.client.swarm.workflow.executable_task import ExecutableTask


class TaskTemplate:

    def __init__(self, name: str, tool: str, command_template: str,
                 dag_args: List[str], data_args: List[str], op_args: List[str]
                 ):
        # task template keys
        self.name = name
        self.tool = tool

        # command strucuture
        self.command_template = command_template
        self.dag_args = dag_args
        self.data_args = data_args
        self.op_args = op_args

    @property
    def template_key_set(self):
        return set(
            [i[1] for i in Formatter().parse(self.command_template)
             if i[1] is not None])

    @property
    def dag_args(self):
        return self._dag_args

    @dag_args.setter
    def dag_args(self, val):
        val = set(val)
        if not self.template_key_set.issuperset(val):
            raise ValueError("template_key_set must be a superset of dag_args")
        self._structural_args = val

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

    def get_command(self, **kwargs):
        return self.command_template.format(**kwargs)


def _command(self):
    return self.task_template.get_command(**self.kwargs)


class Tool:

    def __init__(self, name="unknown"):
        self.name = name

    def get_task_type(
            self, task_class_name, command_template: str,
            dag_args: List[str], data_args: List[str], op_args: List[str]):
        tt = TaskTemplate(task_class_name, self.name, command_template,
                          dag_args, data_args, op_args)
        task_type = type(task_class_name, (ExecutableTask,),
                         {"mk_cmd": _command,
                          "task_template": tt})
        return task_type
