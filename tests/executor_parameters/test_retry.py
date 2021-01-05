import collections
from multiprocessing import Process
import time

import pytest


def hot_resumable_workflow():
    from jobmon.client.api import Tool, ExecutorParameters

    # set up tool and task template
    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"]
    )

    # prepare first workflow
    executor_parameters = ExecutorParameters(
        max_runtime_seconds=180,
        num_cores=1,
        queue='all.q',
        executor_class="SGEExecutor"
    )
    tasks = []
    for i in range(2):
        t = tt.create_task(executor_parameters=executor_parameters, time=10)
        tasks.append(t)
    workflow = unknown_tool.create_workflow(name="hot_resume", workflow_args="foo")
    workflow.set_executor(executor_class="SGEExecutor", project="proj_scicomp")
    workflow.add_tasks(tasks)
    return workflow


def run_hot_resumable_workflow():
    workflow = hot_resumable_workflow()
    workflow.run(seconds_until_timeout=1)


@pytest.mark.integration_sge
def test_hot_resume(db_cfg, client_env):

    p1 = Process(target=run_hot_resumable_workflow)
    p1.start()
    p1.join()

    workflow = hot_resumable_workflow()

    # we need to time out early because the original job will never finish
    workflow.run(resume=True, reset_running_jobs=False)
