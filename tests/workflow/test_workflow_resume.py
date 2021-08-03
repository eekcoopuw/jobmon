import collections
import os
import sys
import time
from multiprocessing import Process

from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (ResumeSet, WorkflowAlreadyExists, WorkflowNotResumable)
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.tool import Tool

from mock import patch

import pytest


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
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash
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


def test_fail_one_task_resume(db_cfg, tool, task_template_fail_one, tmpdir):
    """test that a workflow with a task that fails. The workflow is resumed and
    the task then finishes successfully and the workflow runs to completion"""

    # create workflow and execute
    workflow1 = tool.create_workflow(name="fail_one_task_resume")
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
    workflow_run_status = workflow1.run()

    assert workflow_run_status == WorkflowRunStatus.ERROR
    assert len(workflow1.task_errors) == 1

    # set workflow args and name to be identical to previous workflow
    workflow2 = tool.create_workflow(name=workflow1.name,
                                     workflow_args=workflow1.workflow_args)
    t2 = task_template_fail_one.create_task(
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="")  # fail bool is not set. workflow should succeed
    workflow2.add_tasks([t2])
    workflow2.bind()

    with pytest.raises(WorkflowAlreadyExists):
        workflow2.run()

    workflow_run_status = workflow2.run(resume=True)

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow1.workflow_id == workflow2.workflow_id


def get_two_wave_tasks(task_template):
    tasks = []

    wave_1 = []
    for i in range(3):
        tm = 5 + i
        t = task_template.create_task(arg=f"sleep {tm}")
        tasks.append(t)
        wave_1.append(t)

    for i in range(3):
        tm = 8 + i
        t = task_template.create_task(arg=f"sleep {tm}", upstream_tasks=wave_1)
        tasks.append(t)
    return tasks


class MockDistributorProc:

    def is_alive(self):
        return True


def test_cold_resume(tool, task_template):
    """"""

    # prepare first workflow
    workflow1 = tool.create_workflow(name="cold_resume")
    workflow1.add_tasks(get_two_wave_tasks(task_template))

    # create a distributor and start up the first 3 jobs
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    distributor_service = DistributorService(
        workflow1.workflow_id, wfr1.workflow_run_id,
        MultiprocessDistributor(parallelism=3),
        requester=workflow1.requester
    )
    swarm = SwarmWorkflowRun(workflow_id=workflow1.workflow_id,
                             workflow_run_id=wfr1.workflow_run_id,
                             tasks=list(workflow1.tasks.values()),
                             requester=workflow1.requester)
    swarm.update_status(WorkflowRunStatus.RUNNING)
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())
    distributor_service.distributor.start()
    distributor_service.heartbeat()
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    swarm.block_until_newly_ready_or_all_done()

    # create new workflow run, causing the old one to reset. resume timeout is
    # 1 second meaning this workflow run will not actually be created
    with pytest.raises(WorkflowNotResumable):
        workflow2 = tool.create_workflow(name=workflow1.name,
                                         workflow_args=workflow1.workflow_args)
        workflow2.add_tasks(get_two_wave_tasks(task_template))
        workflow2.bind()
        workflow2._create_workflow_run(resume=True, resume_timeout=1)

    # test if resume signal is received
    with pytest.raises(ResumeSet):
        distributor_service.run_distributor()
    assert distributor_service.distributor.started is False

    # get internal state of workflow run. at least 1 task should have finished
    assert len(swarm.all_done) > 1

    # set workflow run to terminated
    swarm.terminate_workflow_run()

    # now resume it till done
    # prepare first workflow
    workflow3 = tool.create_workflow(
        name=workflow1.name,
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
        workflow_args=workflow1.workflow_args
    )
    workflow3.add_tasks(get_two_wave_tasks(task_template))
    workflow_run_status = workflow3.run(resume=True)

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow3._num_newly_completed >= 3  # number of newly completed tasks


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


def test_hot_resume(db_cfg, client_env):
    p1 = Process(target=run_hot_resumable_workflow)
    p1.start()

    # poll until we determine that the workflow is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        max_sleep = 180  # 3 min max till test fails
        slept = 0

        while status != "R" and slept <= max_sleep:
            time.sleep(5)
            slept += 5

            q = """
                SELECT
                    workflow.status
                FROM
                    workflow
                WHERE
                    workflow.name = 'hot_resume'
            """
            status = session.execute(q).fetchone()
            if status is not None:
                status = status[0]

    if status != "R":
        raise Exception("Workflow never started. Test failed")

    # we need to time out early because the original job will never finish
    with pytest.raises(RuntimeError):
        workflow = hot_resumable_workflow()
        workflow.bind()
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


def test_hot_resume_2(tool, task_template):
    workflow1 = tool.create_workflow(name="hot_resume")
    tasks = []
    for i in range(6):
        t = task_template.create_task(arg=f"sleep {10 + i}")
        tasks.append(t)
    workflow1.add_tasks(tasks)

    # create a workflow and run the first 3 jobs
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    distributor_service1 = DistributorService(
        workflow1.workflow_id, wfr1.workflow_run_id,
        MultiprocessDistributor(parallelism=3),
        requester=workflow1.requester
    )
    swarm1 = SwarmWorkflowRun(workflow_id=workflow1.workflow_id,
                              workflow_run_id=wfr1.workflow_run_id,
                              tasks=list(workflow1.tasks.values()),
                              requester=workflow1.requester)
    swarm1.update_status(WorkflowRunStatus.RUNNING)
    swarm1.compute_initial_dag_state()
    list(swarm1.queue_tasks())
    distributor_service1.distributor.start()
    distributor_service1.heartbeat()
    distributor_service1._get_tasks_queued_for_instantiation()
    distributor_service1.distribute()

    # now make another and set a hot resume with a quick timeout
    workflow2 = tool.create_workflow(name="hot_resume", workflow_args=workflow1.workflow_args)
    tasks = []
    for i in range(6):
        t = task_template.create_task(arg=f"sleep {10 + i}")
        tasks.append(t)
    workflow2.add_tasks(tasks)
    workflow2.bind()
    with pytest.raises(WorkflowNotResumable):
        wfr2 = workflow2._create_workflow_run(resume=True, reset_running_jobs=False,
                                              resume_timeout=1)

    # now check that resume is set and terminate current workflow run.
    with pytest.raises(ResumeSet):
        distributor_service1.run_distributor()
    swarm1.terminate_workflow_run()

    breakpoint()

    # distributor_service = DistributorService(
    #     workflow1.workflow_id, wfr1.workflow_run_id,
    #     SequentialDistributor(),
    #     requester=workflow1.requester
    # )
    # swarm = SwarmWorkflowRun(workflow_id=workflow1.workflow_id,
    #                          workflow_run_id=wfr1.workflow_run_id,
    #                          tasks=list(workflow1.tasks.values()),
    #                          requester=workflow1.requester)
    # swarm.update_status(WorkflowRunStatus.RUNNING)
    # swarm.compute_initial_dag_state()
    # list(swarm.queue_tasks())
    # distributor_service.distributor.start()
    # distributor_service.heartbeat()
    # distributor_service._get_tasks_queued_for_instantiation()
    # distributor_service.distribute()


def test_stopped_resume(tool, task_template):
    """test that a workflow with two task where the workflow is stopped with a
    keyboard interrupt mid stream. The workflow is resumed and
    the tasks then finishes successfully and the workflow runs to completion"""

    workflow1 = tool.create_workflow(name="stopped_resume")
    t1 = task_template.create_task(arg="echo t1")
    t2 = task_template.create_task(arg="echo t2", upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])

    # start up the first task. patch so that it fails with a keyboard interrupt
    workflow1._fail_after_n_executions = 1
    with pytest.raises(KeyboardInterrupt):
        with patch("jobmon.client.swarm.workflow_run.ValueError") as fail_error:
            fail_error.side_effect = KeyboardInterrupt
            # will ask if we want to exit. answer is 'y'
            with patch('builtins.input') as input_patch:
                input_patch.return_value = 'y'
                workflow1.run()

    # now resume it
    workflow1 = tool.create_workflow(
        name="stopped_resume",
        workflow_args=workflow1.workflow_args)
    t1 = task_template.create_task(arg="echo t1")
    t2 = task_template.create_task(arg="echo t2", upstream_tasks=[t1])
    workflow1.add_tasks([t1, t2])
    wfrs2 = workflow1.run(resume=True)

    assert wfrs2 == WorkflowRunStatus.DONE
