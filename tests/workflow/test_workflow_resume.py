import collections
import os
import sys
import time
from multiprocessing import Process

from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (ResumeSet, WorkflowAlreadyExists, WorkflowNotResumable)

from mock import patch

import pytest


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


def test_fail_one_task_resume(db_cfg, client_env, tmpdir):
    """test that a workflow with a task that fails. The workflow is resumed and
    the task then finishes successfully and the workflow runs to completion"""
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
    """test that we cannot create 2 workflow runs simultaneously"""
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
    """"""
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.swarm.workflow_run import WorkflowRun
    from jobmon.requester import Requester

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
    requester = Requester(client_env)
    scheduler = TaskInstanceScheduler(workflow1.workflow_id, wfr1.workflow_run_id,
                                      workflow1._executor, requester=requester)
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
            resume_timeout=1
        )

    # test if resume signal is received
    with pytest.raises(ResumeSet):
        scheduler.run_scheduler()
    assert scheduler.executor.started is False

    # get internal state of workflow run. at least 1 task should have finished
    completed, _ = wfr1._block_until_any_done_or_error()
    assert len(completed) > 0

    wfr1.terminate_workflow_run()

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


@pytest.mark.integration_sge
def test_cold_resume_from_failures(db_cfg, client_env):
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor  # noqa F401

    # set up tool and task template
    unknown_tool = Tool()
    tt_a = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"])
    tt_b = unknown_tool.get_task_template(
        template_name="bar",
        command_template="sleep {time}",
        node_args=["time"])
    tt_c = unknown_tool.get_task_template(
        template_name="baz",
        command_template="sleep {time}",
        node_args=["time"])

    # prepare first workflow
    tasks = []
    for i in range(3):
        t = tt_a.create_task(executor_parameters=ExecutorParameters(
            max_runtime_seconds=1),
            time=2+i)
        tasks.append(t)
    for i in range(2):
        t = tt_b.create_task(executor_parameters=ExecutorParameters(
                max_runtime_seconds=30), time=5+i)
        tasks.append(t)
    for i in range(2):
        t = tt_c.create_task(executor_parameters=ExecutorParameters(
                max_runtime_seconds=30), time=8 + i)
        t.add_upstream(tasks[i])
        t.add_upstream(tasks[i+1])
        t.add_upstream(tasks[i+2])
        tasks.append(t)
    workflow1 = unknown_tool.create_workflow(name="cold_resume_fail")
    workflow1.add_tasks(tasks)
    workflow1.run()

    tasks = []
    for i in range(3):
        t = tt_a.create_task(executor_parameters=ExecutorParameters(
            max_runtime_seconds=10),
            time=2 + i)
        tasks.append(t)
    for i in range(2):
        t = tt_b.create_task(executor_parameters=ExecutorParameters(
                max_runtime_seconds=30), time=5 + i)
        tasks.append(t)
    for i in range(2):
        t = tt_c.create_task(executor_parameters=ExecutorParameters(
                max_runtime_seconds=30), time=8 + i)
        t.add_upstream(tasks[i])
        t.add_upstream(tasks[i + 1])
        t.add_upstream(tasks[i + 2])
        tasks.append(t)
    workflow2 = unknown_tool.create_workflow(name="cold_resume_fail",
                                             workflow_args=workflow1.workflow_args)
    workflow2.add_tasks(tasks)
    wfr_2 = workflow2.run(resume=True)
    assert wfr_2.status == WorkflowRunStatus.DONE
    for task in wfr_2.swarm_tasks:
        assert wfr_2.swarm_tasks[task].status == 'D'


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
        t = tt.create_task(
            executor_parameters=ExecutorParameters(executor_class="MultiprocessExecutor"),
            time=60 + i
        )
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
    from jobmon.client.execution.strategies.multiprocess import MultiprocessExecutor
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

        while status not in ["R"] and slept <= max_sleep:
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
        workflow.run(resume=True, reset_running_jobs=False, seconds_until_timeout=200)

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

    task_dict = {}
    for task in tasks:
        task_dict[task[0]] = task[9]
    tasks = list(collections.OrderedDict(sorted(task_dict.items())).values())

    assert "R" in list(tuple(tasks))  # the task left hanging by hot resume
    assert len([status for status in tasks if status == "D"]) == 5


def test_stopped_resume(db_cfg, client_env):
    """test that a workflow with two task where the workflow is stopped with a
    keyboard interrupt mid stream. The workflow is resumed and
    the tasks then finishes successfully and the workflow runs to completion"""
    from jobmon.client.api import Tool, BashTask
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    unknown_tool = Tool()
    workflow1 = unknown_tool.create_workflow(name="stopped_resume")
    t1 = BashTask("echo t1", executor_class="SequentialExecutor")
    t2 = BashTask("echo t2", executor_class="SequentialExecutor",
                  upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])
    workflow1.set_executor(SequentialExecutor())

    # start up the first task. patch so that it fails with a keyboard interrupt
    workflow1._set_fail_after_n_executions(1)
    with patch("jobmon.client.swarm.workflow_run.ValueError") as fail_error:
        fail_error.side_effect = KeyboardInterrupt
        # will ask if we want to exit. answer is 'y'
        with patch('builtins.input') as input_patch:
            input_patch.return_value = 'y'
            wfr1 = workflow1.run()

    assert wfr1.status == WorkflowRunStatus.STOPPED

    # now resume it
    workflow1 = unknown_tool.create_workflow(
        name="stopped_resume", workflow_args=workflow1.workflow_args)
    t1 = BashTask("echo t1", executor_class="SequentialExecutor")
    t2 = BashTask("echo t2", executor_class="SequentialExecutor",
                  upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])
    workflow1.set_executor(SequentialExecutor())
    wfr2 = workflow1.run(resume=True)

    assert wfr2.status == WorkflowRunStatus.DONE
