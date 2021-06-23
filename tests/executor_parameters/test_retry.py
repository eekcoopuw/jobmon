import time

from jobmon.constants import TaskInstanceStatus, WorkflowRunStatus

import pytest


def hot_resumable_workflow():
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.distributor.strategies import sge  # noqa: F401

    # set up tool and task template
    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"]
    )

    # prepare first workflow
    tasks = []
    for i in range(2):
        executor_parameters = ExecutorParameters(
            max_runtime_seconds=10,
            num_cores=1,
            queue='all.q',
            executor_class="SGEExecutor"
        )
        t = tt.create_task(executor_parameters=executor_parameters, time=15 + i)
        tasks.append(t)
    workflow = unknown_tool.create_workflow(name="hot_resume", workflow_args="foo")
    workflow.set_executor(executor_class="SGEExecutor", project="proj_scicomp")
    workflow.add_tasks(tasks)
    return workflow


class MockDistributorProc:

    def is_alive(self):
        return True
