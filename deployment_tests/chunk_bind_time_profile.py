import time
from jobmon.client.workflow import Workflow
from jobmon.client.tool import Tool
from jobmon.client.task_template import TaskTemplate
from jobmon.client.templates.bash_task import BashTask
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.models.workflow_run_status import WorkflowRunStatus
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def node_bind_profile(tasks, chunk_size: int = None):
    times = []
    for _ in range(10):
        if chunk_size:
            wf = Workflow(1, chunk_size=chunk_size)
        else:
            wf = Workflow(1)
        wf.add_tasks(tasks)
        start = time.time()
        wf._bulk_bind_nodes()
        end = time.time()
        times.append(end-start)

    print(f"Binding {len(tasks)} nodes took {sum(times) / len(times)}s on "
          f"average with a chunk size of {chunk_size}")


def task_bind_profile(tasks, chunk_size: int, resume: bool = False):

    times = []
    wf = Workflow(1, chunk_size=chunk_size)
    wf.add_tasks(tasks)
    wf._bind(False)

    for x in range(10):
        # Measure aggregate workflow bind time

        wf.set_executor(proj="proj_scicomp")
        start = time.time()
        wfr = wf._create_workflow_run(resume, False)
        end = time.time()
        if x > 0:
            times.append(end-start)
        else:
            print(f"New workflow bound in {end-start}s")
        # set wfr to failed
        wfr.update_status(WorkflowRunStatus.ERROR)

    print(f"Took avg. of {sum(times) / len(times)}s to resume tasks")


if __name__ == '__main__':

    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="bash_task",
        command_template="{command}",
        node_args=["command"],
        task_args=[],
        op_args=[])
    params = ExecutorParameters(executor_class="DummyExecutor")
    tasks = [
        tt.create_task(command=f"echo {x}", executor_parameters=params) for x in range(50000)
    ]

    # task_bind_profile(tasks, 1)
    # task_bind_profile(tasks, 10)
    task_bind_profile(tasks, 500, False)
