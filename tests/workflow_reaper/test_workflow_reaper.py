import pytest
from mock import patch, PropertyMock
from jobmon.constants import WorkflowRunStatus
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor


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


@pytest.fixture
def base_tool(db_cfg, client_env):
    from jobmon.client.tool import Tool
    return Tool()

@pytest.fixture
def sleepy_task_template(db_cfg, client_env, base_tool):
    tt = base_tool.get_task_template(
        template_name='sleepy_template',
        command_template="sleep {sleep}",
        node_args=['sleep'],
        compute_resources={'sequential': {'queue': 'null.q'}})
    return tt


def test_error_state(db_cfg, requester_no_retry,
                     base_tool, sleepy_task_template):
    """Tests that the workflow reaper successfully checks for error state.
    Suspended state occurs when a workflow run has not logged a heartbeat in a
    give amount of time. The reaper will then transition the workflow to F
    state, it will transition the workflow_run to E state.
    """
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create a workflow with one task set the workflow run status to R. log a heartbeat so it
    # doesn't get reaped
    task1 = sleepy_task_template.create_task(sleep=10, cluster_name='sequential')
    wf1 = base_tool.create_workflow()
    wf1.add_tasks([task1])
    wf1.bind()
    wfr1 = wf1._create_workflow_run()

    seq_distributor = SequentialDistributor()
    distributor1 = DistributorService(wf1.workflow_id, wfr1.workflow_run_id,
                                      seq_distributor, requester=requester_no_retry)
    distributor1.heartbeat()

    # Create a second workflow with one task. Don't log a heartbeat so that it can die
    task2 = sleepy_task_template.create_task(sleep=11, cluster_name='sequential')
    wf2 = base_tool.create_workflow()
    wf2.add_tasks([task2])
    wf2.bind()
    wfr2 = WorkflowRun(
        workflow_id=wf2.workflow_id,
        requester=wf2.requester
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
    workflow1_status = get_workflow_status(db_cfg, wf1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, wf2.workflow_id)

    assert workflow1_status == "R"
    assert workflow2_status == "F"

    # Check that the  workflow run was also moved to the E state
    wfr_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    assert wfr_status == "E"


def test_halted_state(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    """Tests that the workflow reaper successfully checks for suspended state.
    Suspended state occurs when a workflow run is either in C (cold resume) or
    H (hot resume) state. The reaper will then transition the workflow to S
    state, it will not transition the workflow_run.
    """
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create first WorkflowRun and leave it in running state. log a heartbeat so it doesn't
    # get reaped
    task1 = sleepy_task_template.create_task(sleep=10, cluster_name='sequential')
    workflow1 = base_tool.create_workflow()

    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = workflow1._create_workflow_run()
    seq_distributor = SequentialDistributor()
    distributor1 = DistributorService(workflow1.workflow_id, wfr1.workflow_run_id,
                                       seq_distributor, requester=requester_no_retry)
    distributor1.heartbeat()
    wfr1._update_status("R")

    # Create second WorkflowRun and transition to C status
    task2 = sleepy_task_template.create_task(sleep=11, cluster_name='sequential')
    workflow2 = base_tool.create_workflow()

    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = WorkflowRun(
        workflow_id=workflow2.workflow_id,
        requester=workflow2.requester
    )
    wfr2._link_to_workflow(0)
    wfr2._update_status(WorkflowRunStatus.BOUND)
    wfr2._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr2._update_status(WorkflowRunStatus.LAUNCHED)
    wfr2._update_status(WorkflowRunStatus.RUNNING)
    wfr2._update_status(WorkflowRunStatus.COLD_RESUME)

    # Create third WorkflowRun and transition to H status
    task3 = sleepy_task_template.create_task(sleep=12, cluster_name='sequential')
    workflow3 = base_tool.create_workflow()

    workflow3.add_tasks([task3])
    workflow3.bind()
    wfr3 = WorkflowRun(
        workflow_id=workflow3.workflow_id,
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


def test_aborted_state(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun

    # create a workflow without binding the tasks. log a heartbeat so it doesn't get reaped
    task = sleepy_task_template.create_task(sleep=10, cluster_name='sequential')
    task2 = sleepy_task_template.create_task(sleep=11, cluster_name='sequential')
    workflow = base_tool.create_workflow()
    workflow.add_tasks([task, task2])
    workflow.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr = WorkflowRun(workflow.workflow_id, requester=requester_no_retry)
    wfr._link_to_workflow(90)

    # create a workflow without binding the tasks
    workflow1 = base_tool.create_workflow()
    workflow1.add_tasks([task, task2])
    workflow1.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr1 = WorkflowRun(workflow1.workflow_id, requester=requester_no_retry)
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


def test_reaper_version(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun

    # create a workflow without binding the tasks. log a heartbeat so it doesn't get reaped
    task = sleepy_task_template.create_task(sleep=10, cluster_name='sequential')
    task2 = sleepy_task_template.create_task(sleep=11, cluster_name='sequential')
    workflow = base_tool.create_workflow()
    workflow.add_tasks([task, task2])
    workflow.bind()

    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr = WorkflowRun(workflow.workflow_id, requester=requester_no_retry)
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
