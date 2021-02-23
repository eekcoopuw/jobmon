import time
from typing import Dict

import pytest
from jobmon.server.workflow_reaper.reaper_config import WorkflowReaperConfig


def get_workflow_status(db_cfg, workflow_id):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = f"SELECT status FROM workflow WHERE id = {workflow_id}"
        resp = DB.session.execute(query).fetchone()[0]
    return resp


def get_workflow_run_status(db_cfg, wfr_id):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = f"SELECT status FROM workflow_run WHERE id = {wfr_id}"
        resp = DB.session.execute(query).fetchone()[0]
    return resp


def test_error_state(db_cfg, requester_no_retry):
    """Tests that the workflow reaper successfully checks for error state.
    Suspended state occurs when a workflow run has not logged a heartbeat in a
    give amount of time. The reaper will then transition the workflow to F
    state, it will transition the workflow_run to E state.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create a workflow with one task set the workflow run status to R
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow(name="error_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()

    wfr1.update_status("R")

    # Create a second workflow with one task and set the workflow run to D
    task2 = BashTask("sleep 11")
    workflow2 = UnknownWorkflow("error_workflow_2", executor_class="SequentialExecutor")

    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = workflow2._create_workflow_run()
    wfr2.update_status("R")
    wfr2.update_status("D")

    # Call the reaper, with a short loss_threshold, to trigger the reaper to
    # move the workflow run in to error state
    reaper = WorkflowReaper(poll_interval_minutes=1, loss_threshold=1 / 20,
                            requester=requester_no_retry)
    i = 0
    while i < 10:
        time.sleep(10)
        i += 1
        lost = reaper._get_lost_workflow_runs()
        if lost:
            break
    reaper._error_state()

    # Check that the workflow that exceeded the loss threshold now has an F
    # status, make sure the other workflow is untouched
    workflow1_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, workflow2.workflow_id)

    assert workflow1_status == "F"
    assert workflow2_status == "D"

    # # Check that the  workflow run was also moved to the E state
    wfr_status = get_workflow_run_status(db_cfg, wfr1.workflow_run_id)
    assert wfr_status == "E"


def test_suspended_state(db_cfg, requester_no_retry):
    """Tests that the workflow reaper successfully checks for suspended state.
    Suspended state occurs when a workflow run is either in C (cold resume) or
    H (hot resume) state. The reaper will then transition the workflow to S
    state, it will not transition the workflow_run.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create first WorkflowRun and leave it in running state
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow("suspended_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()

    wfr1.update_status("R")

    # Create second WorkflowRun and tranistion to C status
    task2 = BashTask("sleep 11")
    workflow2 = UnknownWorkflow("suspended_workflow_2", executor_class="SequentialExecutor")

    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = workflow2._create_workflow_run()
    wfr2.update_status("R")
    wfr2.update_status("C")

    # Create third WorkflowRun and transition to H status
    task3 = BashTask("sleep 12")
    workflow3 = UnknownWorkflow("suspended_workflow_3", executor_class="SequentialExecutor")

    workflow3.add_tasks([task3])
    workflow3.bind()
    wfr3 = workflow3._create_workflow_run()
    wfr3.update_status("R")
    wfr3.update_status("H")

    # The reaper will only move workflows to suspended that have a heartbeat rate greater than
    # (heatbeat_interval * report_by_buffer). Sleep, so the wf is eligible to be reaped.
    cfg = WorkflowReaperConfig.from_defaults()
    time_out = cfg.workflow_run_heartbeat_interval * cfg.task_instance_report_by_buffer
    time.sleep(time_out * 1.2)

    # Call workflow reaper suspended state
    reaper = WorkflowReaper(5, 5, requester=requester_no_retry)
    reaper._terminated_state()

    # Check that the workflow runs are in the same state (1 R, 2 T)
    # and that there are two workflows in S state and one still in R state
    wfr1_status = get_workflow_run_status(db_cfg, wfr1.workflow_run_id)
    wfr2_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    wfr3_status = get_workflow_run_status(db_cfg, wfr3.workflow_run_id)
    assert wfr1_status == "R"
    assert wfr2_status == "T"
    assert wfr3_status == "T"

    workflow1_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, workflow2.workflow_id)
    workflow3_status = get_workflow_status(db_cfg, workflow3.workflow_id)
    assert workflow1_status == "R"
    assert workflow2_status == "H"
    assert workflow3_status == "H"


@pytest.mark.skip(reason="need to implement heartbeat")
def test_aborted_state(db_cfg, requester_no_retry):
    """Tests that the workflow reaper successfully checks for aborted state.
    Aborted state occurs when the workflow_run is in the G state and the last
    task associated with it has a status_date that is more than two minutes
    old. This assures that the workflow run is currently not binding. The
    reaper will then transition the workflow and workflow_run to the "A"
    state"""

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.client.swarm.swarm_task import SwarmTask

    # Create two tasks and add them to a workflow
    task = BashTask("sleep 8")
    task2 = BashTask("sleep 7")
    workflow = UnknownWorkflow("aborted_workflow_1",
                               executor_class="SequentialExecutor")
    workflow.add_tasks([task, task2])
    workflow.bind()

    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr = WorkflowRun(workflow.workflow_id, executor_class="SequentialExecutor",
                      requester=requester_no_retry)

    swarm_tasks: Dict[int, SwarmTask] = {}
    for task in workflow.tasks.values():
        task.workflow_id = workflow.workflow_id
        task.bind()
        swarm_task = SwarmTask(
            task_id=task.task_id,
            status=task.initial_status,
            task_args_hash=task.task_args_hash,
            executor_parameters=task.executor_parameters,
            max_attempts=task.max_attempts
        )
        swarm_tasks[task.task_id] = swarm_task

    wfr.swarm_tasks = swarm_tasks

    # Sleep for five seconds and update the second tasks status_date.
    # This assures that the two tasks have differing times.
    time.sleep(5)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
            UPDATE task
            SET task.status_date = CURRENT_TIMESTAMP()
            WHERE id = :task_id
        """
        DB.session.execute(query, {"task_id": task2.task_id})
        DB.session.commit()

    # The reaper only views tasks that have a status_date that is more than
    # two minutes ago. Wait slightly longer than that to assure the reaper
    # views it as a workflow run that is eligible to move to A state
    time.sleep(130)

    # Call aborted state logic
    reaper = WorkflowReaper(5, 5, requester=requester_no_retry)
    reaper._aborted_state(wfr.workflow_run_id)

    # Check that the workflow_run and workflow have both been moved to the
    # "A" state.
    workflow_status = get_workflow_status(db_cfg, workflow.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr.workflow_run_id)
    assert workflow_status == "A"
    assert workflow_run_status == "A"


@pytest.mark.skip(reason="need to implement heartbeat")
def test_aborted_state_null_case(db_cfg, requester_no_retry):
    from jobmon.client.api import BashTask, UnknownWorkflow
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # create a workflow without binding the tasks
    task = BashTask("foo")
    task2 = BashTask("bar")
    workflow = UnknownWorkflow("aborted_workflow_1", executor_class="SequentialExecutor")
    workflow.add_tasks([task, task2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    time.sleep(5)

    # Call aborted state logic
    reaper = WorkflowReaper(5, 5, requester=requester_no_retry)
    reaper._aborted_state(wfr.workflow_run_id, 1)

    # Check that the workflow_run and workflow have both been moved to the
    # "A" state.
    workflow_status = get_workflow_status(db_cfg, workflow.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr.workflow_run_id)
    assert workflow_status == "A"
    assert workflow_run_status == "A"
