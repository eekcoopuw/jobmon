from typing import List

from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.client.swarm.workflow.task_template import TaskTemplate


def _command(self):
    return self.task_template.get_command(**self.kwargs)


class Tool:

    def __init__(self, name="unknown"):
        self.name = name

    def get_task_type(
            self, task_class_name, command_template: str,
            node_args: List[str], data_args: List[str], op_args: List[str]):
        tt = TaskTemplate(task_class_name, self.name, command_template,
                          node_args, data_args, op_args)
        task_type = type(task_class_name, (ExecutableTask,),
                         {"mk_cmd": _command,
                          "task_template": tt})
        return task_type

    def get_workflow(self):
        pass


if __name__ == "__main__":
    import sys

    como_tool = Tool("como")
    IncidenceTask = como_tool.get_task_type(
        task_class_name="IncidenceTask",
        command_template=(
            "{python} "
            "{script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--sex_id {sex_id} "
            "--n_processes {n_processes}"),
        node_args=["location_id", "sex_id"],
        data_args=["como_dir"],
        op_args=["python", "script", "n_processes"])
    for location_id in [1, 2, 3]:
        for sex_id in [1, 2]:
            task = IncidenceTask(
                python=sys.executable,
                script="foo.py",
                como_dir="/ihme/centralcomp/como/123",
                location_id=location_id,
                sex_id=sex_id,
                n_processes=25,
                executor_class="DummyExecutor")
            print(task.command)
