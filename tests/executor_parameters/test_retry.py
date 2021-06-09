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


@pytest.mark.integration_sge
@pytest.mark.skip()
def test_hot_resume_with_adjusting_resource(db_cfg, client_env):
    from jobmon.client.distributor.task_instance_distributor import \
        TaskInstanceDistributor
    from jobmon.requester import Requester

    # set up initial run
    workflow = hot_resumable_workflow()

    # bind tasks and
    workflow._bind()
    wfr = workflow._create_workflow_run()
    requester = Requester(client_env)
    distributor = TaskInstanceDistributor(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester,
                                      task_heartbeat_interval=10, report_by_buffer=1.1)
    try:
        wfr.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)
    except RuntimeError:
        pass
    distributor._get_tasks_queued_for_instantiation()
    distributor.distribute()

    # wait till we enter adjusting then move on to hot resumed workflow
    while not distributor._to_reconcile:
        time.sleep(5)
        distributor._get_lost_task_instances()

    for ti in distributor._to_reconcile:
        app_route = f"/distributor/task_instance/{ti.task_instance_id}/log_error_reconciler"
        return_code, response = ti.requester.send_request(
            app_route=app_route,
            message={
                "error_message": "foo",
                "error_state": TaskInstanceStatus.RESOURCE_ERROR,
                "executor_id": ti.executor_id
            },
            request_type='post',
        )

    swarm_tasks = wfr._task_status_updates()
    _, _, adjusting = wfr._parse_adjusting_done_and_errors(swarm_tasks)

    assert adjusting

    # hot resume should pick up the adjusting task and finish
    workflow = hot_resumable_workflow()
    wfr = workflow.run(resume=True, reset_running_jobs=False)
    assert wfr.status == WorkflowRunStatus.DONE
