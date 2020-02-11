from multiprocessing import Process
import os
import time
import sys

import pytest

from jobmon.exceptions import (WorkflowAlreadyExists, WorkflowNotResumable,
                               ResumeSet)
from jobmon.models.workflow_run_status import WorkflowRunStatus


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


def test_fail_one_task_resume(db_cfg, client_env, tmpdir):
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    unknown_tool = Tool()
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template=(
            "{python} "
            "{script} "
            "--sleep_secs {sleep_secs} "
            "--output_file_path {output_file_path} "
            "--name {name} "
            "{fail_always}"),
        node_args=["name"],
        task_args=["sleep_secs", "output_file_path"],
        op_args=["python", "script", "fail_always"])

    # create workflow and execute
    workflow1 = unknown_tool.create_workflow(name="fail_one_task_resume")
    workflow1.set_executor(SequentialExecutor())
    t1 = tt.create_task(
        executor_parameters=ExecutorParameters(
            executor_class="SequentialExecutor"),
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="--fail_always")  # fail bool is set
    workflow1.add_tasks([t1])
    wfr1 = workflow1.run()

    assert wfr1.status == WorkflowRunStatus.ERROR
    assert len(wfr1.all_error) == 1

    # set workflow args and name to be identical to previous workflow
    workflow2 = unknown_tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args)
    workflow2.set_executor(SequentialExecutor())
    t2 = tt.create_task(
        executor_parameters=ExecutorParameters(
            executor_class="SequentialExecutor"),
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="")   # fail bool is not set. workflow should succeed
    workflow2.add_tasks([t2])

    with pytest.raises(WorkflowAlreadyExists):
        workflow2.run()

    wfr2 = workflow2.run(resume=True)

    assert wfr2.status == WorkflowRunStatus.DONE
    assert workflow1.workflow_id == workflow2.workflow_id
    assert wfr2.workflow_run_id != wfr1.workflow_run_id


def test_multiple_active_race_condition(db_cfg, client_env):
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"])

    # create initial workflow
    t1 = tt.create_task(executor_parameters=ExecutorParameters(
                            executor_class="SequentialExecutor"),
                        time=1)
    workflow1 = unknown_tool.create_workflow(name="created_race_condition")
    workflow1.set_executor(SequentialExecutor())
    workflow1.add_tasks([t1])
    workflow1._bind()
    workflow1._create_workflow_run()

    # create identical workflow
    t2 = tt.create_task(executor_parameters=ExecutorParameters(
                            executor_class="SequentialExecutor"),
                        time=1)
    workflow2 = unknown_tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args)
    workflow2.set_executor(SequentialExecutor())
    workflow2.add_tasks([t2])
    workflow2._bind(resume=True)
    with pytest.raises(WorkflowNotResumable):
        workflow2._create_workflow_run()


class MockSchedulerProc:

    def is_alive(self):
        return True


def test_cold_resume(db_cfg, client_env):
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.swarm.workflow_run import WorkflowRun

    # set up tool and task template
    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"])

    # prepare first workflow
    tasks = []
    for i in range(6):
        t = tt.create_task(executor_parameters=ExecutorParameters(
                               executor_class="MultiprocessExecutor"),
                           time=5 + i)
        tasks.append(t)
    workflow1 = unknown_tool.create_workflow(name="cold_resume")
    workflow1.set_executor(MultiprocessExecutor(parallelism=3))
    workflow1.add_tasks(tasks)

    # create an in memory scheduler and start up the first 3 jobs
    workflow1._bind()
    wfr1 = workflow1._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow1.workflow_id,
                                      wfr1.workflow_run_id,
                                      workflow1._executor)
    with pytest.raises(RuntimeError):
        wfr1.execute_interruptible(MockSchedulerProc(),
                                   seconds_until_timeout=1)
    scheduler.executor.start()
    scheduler.heartbeat()
    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

    time.sleep(6)

    # create new workflow run, causing the old one to reset. resume timeout is
    # 1 second meaning this workflow run will not actually be created
    with pytest.raises(WorkflowNotResumable):
        WorkflowRun(
            workflow_id=workflow1.workflow_id,
            executor_class=workflow1._executor.__class__.__name__,
            resume=True,
            reset_running_jobs=True,
            requester=workflow1.requester,
            resume_timeout=1)

    # test if resume signal is recieved
    with pytest.raises(ResumeSet):
        scheduler.run_scheduler()

    # get internal state of workflow run. first 3 tasks or more should be done
    completed, _ = wfr1._block_until_any_done_or_error()
    assert len(completed) >= 3

    # now resume it till done
    # prepare first workflow
    tasks = []
    for i in range(6):
        t = tt.create_task(executor_parameters=ExecutorParameters(
                               executor_class="MultiprocessExecutor"),
                           time=5 + i)
        tasks.append(t)
    workflow2 = unknown_tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args)
    workflow2.set_executor(MultiprocessExecutor(parallelism=3))
    workflow2.add_tasks(tasks)
    wfr2 = workflow2.run(resume=True)

    assert wfr2.status == WorkflowRunStatus.DONE
    assert wfr2.completed_report[0] > 0  # number of newly completed tasks


def hot_resumable_workflow():
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # set up tool and task template
    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"])

    # prepare first workflow
    tasks = []
    for i in range(6):
        t = tt.create_task(executor_parameters=ExecutorParameters(
                               executor_class="MultiprocessExecutor"),
                           time=60 + i)
        tasks.append(t)
    workflow = unknown_tool.create_workflow(name="hot_resume",
                                            workflow_args="foo")
    workflow.set_executor(SequentialExecutor())
    workflow.add_tasks(tasks)
    return workflow


def run_hot_resumable_workflow():
    workflow = hot_resumable_workflow()
    workflow.run()


def test_hot_resume(db_cfg, client_env):
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor
    p1 = Process(target=run_hot_resumable_workflow)
    p1.start()

    # avoid race condition
    time.sleep(5)

    workflow = hot_resumable_workflow()
    workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow._bind(resume=True)

    # poll until we determine that the workflow is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        max_sleep = 180  # 3 min max till test fails
        slept = 0

        while status != "R" and slept <= max_sleep:
            q = """
                SELECT
                    workflow.status
                FROM
                    workflow
                WHERE
                    workflow.id = {}
            """.format(workflow.workflow_id)
            status = session.execute(q).fetchone()[0]
            time.sleep(5)
            slept += 5

    if status != "R":
        raise Exception("Workflow never started. Test failed")

    # we need to time out early because the original job will never finish
    with pytest.raises(RuntimeError):
        workflow.run(resume=True, reset_running_jobs=False,
                     seconds_until_timeout=70)

    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        q = """
            SELECT
                task.*
            FROM
                task
            WHERE
                workflow_id = {}
        """.format(workflow.workflow_id)
        tasks = session.execute(q).fetchall()

    assert tasks[0][9] == "R"  # the task before resume
    assert tasks[1][9] == "D"  # a resume task that finished before timeout
    assert tasks[2][9] == "D"  # a resume task that finished before timeout
    assert tasks[3][9] == "D"  # a resume task that finished before timeout
    assert tasks[4][9] == "Q"  # workflow timed out. task deleted
    assert tasks[5][9] == "Q"  # workflow timed out. task deleted
