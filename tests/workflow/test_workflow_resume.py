import collections
import os
import sys
import time
from multiprocessing import Process

from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (ResumeSet, WorkflowAlreadyExists, WorkflowNotResumable)

from mock import patch

import pytest

from jobmon.client.task import Task
from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(cluster_name="sequential",
                                                 compute_resources={"queue": "null.q"})
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


@pytest.fixture
def task_template_fail_one(tool):
    tt = tool.get_task_template(
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
    return tt


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


def test_fail_one_task_resume(db_cfg, client_env, task_template_fail_one, tmpdir):
    """test that a workflow with a task that fails. The workflow is resumed and
    the task then finishes successfully and the workflow runs to completion"""

    tool = Tool()
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash

    # create workflow and execute
    workflow1 = tool.create_workflow(name="fail_one_task_resume",
                                    default_cluster_name = "sequential",
                                    default_compute_resources_set = {"sequential": {"queue": "null.q"}})
    t1 = task_template_fail_one.create_task(
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="--fail_always")
    workflow1.add_tasks([t1])
    workflow1.bind()
    wfrs1 = workflow1.run()

    assert wfrs1 == WorkflowRunStatus.ERROR
    assert len(workflow1._last_workflowrun.all_error) == 1

    # set workflow args and name to be identical to previous workflow
    workflow2 = tool.create_workflow(name=workflow1.name,
                                     default_cluster_name="sequential",
                                     default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                     workflow_args=workflow1.workflow_args)
    t2 = task_template_fail_one.create_task(
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="") # fail bool is not set. workflow should succeed
    workflow2.add_tasks([t2])
    workflow2.bind()

    with pytest.raises(WorkflowAlreadyExists):
        workflow2.run()

    wfrs2 = workflow2.run(resume=True)

    assert wfrs2 == WorkflowRunStatus.DONE
    assert workflow1.workflow_id == workflow2.workflow_id
    assert workflow2._last_workflowrun.workflow_run_id != workflow1._last_workflowrun.workflow_run_id


def test_multiple_active_race_condition(db_cfg, client_env, task_template):
    """test that we cannot create 2 workflow runs simultaneously"""

    tool = Tool()

    # create initial workflow
    t1 = task_template.create_task(
         arg="sleep 1")
    workflow1 = tool.create_workflow(name="created_race_condition",
                                     default_cluster_name="sequential",
                                     default_compute_resources_set={"sequential": {"queue": "null.q"}})
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._create_workflow_run()

    # create identical workflow
    t2 = task_template.create_task(
         arg="sleep 1")
    workflow2 = tool.create_workflow(name=workflow1.name,
                                     default_cluster_name="sequential",
                                     default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                     workflow_args=workflow1.workflow_args)
    workflow2.add_tasks([t2])
    workflow2.bind()
    with pytest.raises(WorkflowNotResumable):
        workflow2._create_workflow_run(resume=True)


class MockDistributorProc:

    def is_alive(self):
        return True


def test_cold_resume(db_cfg, client_env, task_template):
    """"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.requester import Requester

    # set up tool and task template
    tool = Tool()

    # prepare first workflow
    tasks = []
    for i in range(6):
        tm = 5 + i
        t = task_template.create_task(
                arg=f"sleep {tm}")
        tasks.append(t)
    workflow1 = tool.create_workflow(name="cold_resume",
                                     default_cluster_name="multiprocess",
                                     default_compute_resources_set={"multiprocess": {"queue": "null.q"}})
    workflow1.add_tasks(tasks)

    # create an in memory distributor and start up the first 3 jobs
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    requester = Requester(client_env)
    distributor_service = DistributorService(workflow1.workflow_id, wfr1.workflow_run_id,
                                      "multiprocess", requester=requester)
    with pytest.raises(RuntimeError):
        wfr1.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)
    distributor_service.distributor.start()
    distributor_service.heartbeat()
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    time.sleep(6)
    # import pdb; pdb.set_trace()

    # create new workflow run, causing the old one to reset. resume timeout is
    # 1 second meaning this workflow run will not actually be created
    with pytest.raises(WorkflowNotResumable):
        workflow2 = tool.create_workflow(
            name=workflow1.name,
            default_cluster_name="multiprocess",
            default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
            workflow_args=workflow1.workflow_args)
        workflow2.add_tasks(tasks)
        workflow2.bind()
        workflow2._create_workflow_run(resume=True, resume_timeout=1)

    # test if resume signal is received
    with pytest.raises(ResumeSet):
        distributor_service.run_distributor()
    assert distributor_service.distributor.started is False
    # get internal state of workflow run. at least 1 task should have finished
    completed, _ = wfr1._block_until_any_done_or_error()
    assert len(completed) > 0

    # set workflow run to terminated
    wfr1.terminate_workflow_run()

    # now resume it till done
    # prepare first workflow
    tasks = []
    for i in range(6):
        tm = 5 + i
        t = task_template.create_task(
                arg=f"sleep {tm}")
        tasks.append(t)
    workflow3 = tool.create_workflow(
            name=workflow1.name,
            default_cluster_name="multiprocess",
            default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
            workflow_args=workflow1.workflow_args)
    workflow3.add_tasks(tasks)
    workflow3.bind()
    wfrs3 = workflow3.run(resume=True)

    assert wfrs3 == WorkflowRunStatus.DONE
    assert workflow3._last_workflowrun.completed_report[0] > 0  # number of newly completed tasks

def hot_resumable_workflow():

    # set up tool and task template
    unknown_tool = Tool()
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template="sleep {time}",
        node_args=["time"])

    # prepare first workflow
    tasks = []
    for i in range(6):
        t = tt.create_task(time=60 + i)
        tasks.append(t)
    workflow = unknown_tool.create_workflow(name="hot_resume",
                                            default_cluster_name="sequential",
                                            default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                            workflow_args="foo")
    workflow.add_tasks(tasks)
    workflow.bind()
    return workflow


def run_hot_resumable_workflow():
    workflow = hot_resumable_workflow()
    workflow.run()


# def test_hot_resume(db_cfg, client_env):
#     p1 = Process(target=run_hot_resumable_workflow)
#     p1.start()
#
#     # poll until we determine that the workflow is running
#     session = db_cfg["DB"].session
#     with db_cfg["app"].app_context():
#         status = ""
#         max_sleep = 180  # 3 min max till test fails
#         slept = 0
#
#         while status != "R" and slept <= max_sleep:
#             time.sleep(5)
#             slept += 5
#
#             q = """
#                 SELECT
#                     workflow.status
#                 FROM
#                     workflow
#                 WHERE
#                     workflow.name = 'hot_resume'
#             """
#             status = session.execute(q).fetchone()
#             if status is not None:
#                 status = status[0]
#
#     if status != "R":
#         raise Exception("Workflow never started. Test failed")
#
#     # we need to time out early because the original job will never finish
#     with pytest.raises(RuntimeError):
#         workflow = hot_resumable_workflow()
#         workflow.run(resume=True, reset_running_jobs=False, seconds_until_timeout=200)
#
#     session = db_cfg["DB"].session
#     with db_cfg["app"].app_context():
#         q = """
#             SELECT
#                 task.*
#             FROM
#                 task
#             WHERE
#                 workflow_id = {}
#         """.format(workflow.workflow_id)
#         tasks = session.execute(q).fetchall()
#
#     task_dict = {}
#     for task in tasks:
#         task_dict[task[0]] = task[9]
#     tasks = list(collections.OrderedDict(sorted(task_dict.items())).values())
#
#     assert "R" in list(tuple(tasks))  # the task left hanging by hot resume
#     assert len([status for status in tasks if status == "D"]) == 5

def test_stopped_resume(db_cfg, client_env, task_template):
    """test that a workflow with two task where the workflow is stopped with a
    keyboard interrupt mid stream. The workflow is resumed and
    the tasks then finishes successfully and the workflow runs to completion"""

    tool = Tool()
    workflow1 = tool.create_workflow(name="stopped_resume",
                                    default_cluster_name="sequential",
                                    default_compute_resources_set={"sequential": {"queue": "null.q"}})
    t1 = task_template.create_task(
                arg="echo t1")
    t2 = task_template.create_task(
                arg="echo t2", upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])
    workflow1.bind()

    # start up the first task. patch so that it fails with a keyboard interrupt
    workflow1._set_fail_after_n_executions(1)
    with patch("jobmon.client.swarm.workflow_run.ValueError") as fail_error:
        fail_error.side_effect = KeyboardInterrupt
        # will ask if we want to exit. answer is 'y'
        with patch('builtins.input') as input_patch:
            input_patch.return_value = 'y'
            wfrs1 = workflow1.run()

    assert wfrs1 == WorkflowRunStatus.STOPPED

    # now resume it
    workflow1 = tool.create_workflow(
        name="stopped_resume",
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
        workflow_args=workflow1.workflow_args)
    t1 = task_template.create_task(
                arg="echo t1")
    t2 = task_template.create_task(
                arg="echo t2",
                  upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])
    workflow1.bind()
    wfrs2 = workflow1.run(resume=True)

    assert wfrs2 == WorkflowRunStatus.DONE
