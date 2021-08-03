from mock import patch, PropertyMock
from jobmon.constants import WorkflowRunStatus


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
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create a workflow with one task set the workflow run status to R. log a heartbeat so it
    # doesn't get reaped
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow(name="error_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    distributor1 = DistributorService(workflow1.workflow_id, wfr1.workflow_run_id,
                                      workflow1._executor, requester=requester_no_retry)
    distributor1.heartbeat()

    # Create a second workflow with one task. Don't log a heartbeat so that it can die
    task2 = BashTask("sleep 11")
    workflow2 = UnknownWorkflow("error_workflow_2", executor_class="SequentialExecutor")
    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = WorkflowRun(
        workflow_id=workflow2.workflow_id,
        executor_class=workflow2._executor.__class__.__name__,
        requester=workflow2.requester
    )
    wfr2._link_to_workflow(0)
    wfr2._update_status(WorkflowRunStatus.BOUND)
    wfr2._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr2._update_status(WorkflowRunStatus.LAUNCHED)
    wfr2._update_status(WorkflowRunStatus.RUNNING)

    # Instantiate reaper, have it check for workflow runs in error state
    reaper = WorkflowReaper(poll_interval_minutes=1, requester=requester_no_retry)
    reaper._error_state()

    # Check that one workflow is running and the other failed
    workflow1_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, workflow2.workflow_id)

    assert workflow1_status == "R"
    assert workflow2_status == "F"

    # Check that the  workflow run was also moved to the E state
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
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create first WorkflowRun and leave it in running state. log a heartbeat so it doesn't
    # get reaped
    task1 = BashTask("sleep 10")
    workflow1 = UnknownWorkflow("suspended_workflow_1", executor_class="SequentialExecutor")

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    distributor1 = DistributorService(workflow1.workflow_id, wfr1.workflow_run_id,
                                       workflow1._executor, requester=requester_no_retry)
    distributor1.heartbeat()
    wfr1.update_status("R")

    # Create second WorkflowRun and tranistion to C status
    task2 = BashTask("sleep 11")
    workflow2 = UnknownWorkflow("suspended_workflow_2", executor_class="SequentialExecutor")

    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = WorkflowRun(
        workflow_id=workflow2.workflow_id,
        executor_class=workflow2._executor.__class__.__name__,
        requester=workflow2.requester
    )
    wfr2._link_to_workflow(0)
    wfr2._update_status(WorkflowRunStatus.BOUND)
    wfr2._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr2._update_status(WorkflowRunStatus.LAUNCHED)
    wfr2._update_status(WorkflowRunStatus.RUNNING)
    wfr2._update_status(WorkflowRunStatus.COLD_RESUME)

    # Create third WorkflowRun and transition to H status
    task3 = BashTask("sleep 12")
    workflow3 = UnknownWorkflow("suspended_workflow_3", executor_class="SequentialExecutor")

    workflow3.add_tasks([task3])
    workflow3.bind()
    wfr3 = WorkflowRun(
        workflow_id=workflow3.workflow_id,
        executor_class=workflow3._executor.__class__.__name__,
        requester=workflow3.requester
    )
    wfr3._link_to_workflow(0)
    wfr3._update_status(WorkflowRunStatus.BOUND)
    wfr3._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr3._update_status(WorkflowRunStatus.LAUNCHED)
    wfr3._update_status(WorkflowRunStatus.RUNNING)
    wfr3._update_status(WorkflowRunStatus.HOT_RESUME)

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
    wfr._link_to_workflow(90)

    # create a workflow without binding the tasks
    workflow1 = UnknownWorkflow("aborted_workflow_2", executor_class="SequentialExecutor")
    workflow1.add_tasks([task, task2])
    workflow1.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr1 = WorkflowRun(workflow1.workflow_id, executor_class="SequentialExecutor",
                       requester=requester_no_retry)
    wfr1._link_to_workflow(0)

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


def test_reaper_version(db_cfg, requester_no_retry):
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
    wfr._link_to_workflow(0)
    wfr._log_heartbeat(0)

    # Check for lost workflow runs
    reaper = WorkflowReaper(5, requester=requester_no_retry)
    reaper_wfrs = reaper._get_lost_workflow_runs(["L"])

    assert len(reaper_wfrs) > 0
    assert wfr.workflow_run_id in [wfr.workflow_run_id for wfr in reaper_wfrs]

    # Mock the version to some nonsense
    with patch.object(WorkflowReaper, '_version', new_callable=PropertyMock) as mock:
        mock.return_value = "foobar"

        no_wfrs = reaper._get_lost_workflow_runs(["L", "C", "H"])
        assert len(no_wfrs) == 0
