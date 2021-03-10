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
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create a workflow with one task set the workflow run status to R. log a heartbeat so it
    # doesn't get reaped
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow(name="error_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    scheduler1 = TaskInstanceScheduler(workflow1.workflow_id, wfr1.workflow_run_id,
                                       workflow1._executor, requester=requester_no_retry)
    scheduler1.heartbeat()

    # Create a second workflow with one task. don't log a heartbeat so that it can die
    task2 = BashTask("sleep 11")
    workflow2 = UnknownWorkflow("error_workflow_2", executor_class="SequentialExecutor")
    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = workflow2._create_workflow_run()
    wfr2.update_status("R")

    # Call the reaper, with a short loss_threshold, to trigger the reaper to
    # move the workflow run in to error state
    reaper = WorkflowReaper(poll_interval_minutes=1, requester=requester_no_retry)
    reaper._error_state()

    # Check that the workflow that exceeded the loss threshold now has an F
    # status, make sure the other workflow is untouched
    workflow1_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, workflow2.workflow_id)

    assert workflow1_status == "R"
    assert workflow2_status == "F"

    # # Check that the  workflow run was also moved to the E state
    wfr_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    assert wfr_status == "E"


def test_halted_state(db_cfg, requester_no_retry):
    """Tests that the workflow reaper successfully checks for suspended state.
    Suspended state occurs when a workflow run is either in C (cold resume) or
    H (hot resume) state. The reaper will then transition the workflow to S
    state, it will not transition the workflow_run.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create first WorkflowRun and leave it in running state. log a heartbeat so it doesn't
    # get reaped
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow("suspended_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    scheduler1 = TaskInstanceScheduler(workflow1.workflow_id, wfr1.workflow_run_id,
                                       workflow1._executor, requester=requester_no_retry)
    scheduler1.heartbeat()
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

    # Call workflow reaper suspended state
    reaper = WorkflowReaper(5, requester=requester_no_retry)
    reaper._halted_state()

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


def test_aborted_state(db_cfg, requester_no_retry):
    from jobmon.client.api import BashTask, UnknownWorkflow
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun

    # create a workflow without binding the tasks. log a heartbeat so it doesn't get reaped
    task = BashTask("foo")
    task2 = BashTask("bar")
    workflow = UnknownWorkflow("aborted_workflow_1", executor_class="SequentialExecutor")
    workflow.add_tasks([task, task2])
    workflow.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr = WorkflowRun(workflow.workflow_id, executor_class="SequentialExecutor",
                      requester=requester_no_retry)
    wfr._link_to_workflow()
    wfr._log_heartbeat()

    # create a workflow without binding the tasks
    workflow1 = UnknownWorkflow("aborted_workflow_2", executor_class="SequentialExecutor")
    workflow1.add_tasks([task, task2])
    workflow1.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr1 = WorkflowRun(workflow1.workflow_id, executor_class="SequentialExecutor",
                       requester=requester_no_retry)
    wfr1._link_to_workflow()

    # Call aborted state logic
    reaper = WorkflowReaper(5, requester=requester_no_retry)
    reaper._aborted_state()

    # Check that the workflow_run and workflow have both been moved to the
    # "A" state.
    workflow_status = get_workflow_status(db_cfg, workflow.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr.workflow_run_id)
    assert workflow_run_status == "L"
    assert workflow_status == "G"

    workflow_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr1.workflow_run_id)
    assert workflow_run_status == "A"
    assert workflow_status == "A"
