import pytest
from mock import patch, PropertyMock

from jobmon.constants import WorkflowRunStatus, WorkflowStatus
from jobmon import __version__


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
        template_name="sleepy_template",
        command_template="sleep {sleep}",
        node_args=["sleep"],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


def test_error_state(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    """Tests that the workflow reaper successfully checks for error state.

    Error state occurs when a workflow run has not logged a heartbeat in a
    give amount of time. The reaper will then transition the workflow to F
    state, it will transition the workflow_run to E state.
    """
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create a workflow with one task set the workflow run status to R. log a heartbeat so it
    # doesn't get reaped
    task1 = sleepy_task_template.create_task(sleep=10)
    wf1 = base_tool.create_workflow()
    wf1.add_tasks([task1])
    wf1.bind()
    wfr1 = WorkflowRun(workflow=wf1, requester=wf1.requester)
    wfr1._link_to_workflow(0)
    wfr1._log_heartbeat(300)
    wfr1._update_status(WorkflowRunStatus.BOUND)
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)

    # Create a second workflow with one task. Don't log a heartbeat so that it can die
    task2 = sleepy_task_template.create_task(sleep=11)
    wf2 = base_tool.create_workflow(name="reaper_error_test", workflow_args="error_v_1")
    wf2.add_tasks([task2])
    wf2.bind()
    wfr2 = WorkflowRun(workflow=wf2, requester=wf2.requester)
    wfr2._link_to_workflow(0)
    wfr2._update_status(WorkflowRunStatus.BOUND)
    wfr2._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr2._update_status(WorkflowRunStatus.LAUNCHED)
    wfr2._update_status(WorkflowRunStatus.RUNNING)

    def mock_slack_notifier(msg: str):
        pass

    # Instantiate reaper, have it check for workflow runs in error state
    reaper = WorkflowReaper(
        poll_interval_seconds=60,
        requester=requester_no_retry,
        wf_notification_sink=mock_slack_notifier,
    )

    msg = reaper._error_state()

    assert (
        f"{__version__} Workflow Reaper transitioned a Workflow to FAILED state and "
        f"associated Workflow Run to ERROR state.\nWorkflow ID: {wf2.workflow_id}\nWorkflow Name: "
        f"reaper_error_test\nWorkflow Args: error_v_1\nWorkflowRun ID: {wfr2.workflow_run_id}"
        in msg
    )

    # Check that one workflow is running and the other failed
    workflow1_status = get_workflow_status(db_cfg, wf1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, wf2.workflow_id)

    assert workflow1_status == WorkflowStatus.RUNNING
    assert workflow2_status == WorkflowStatus.FAILED

    # Check that the  workflow run was also moved to the E state
    wfr_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    assert wfr_status == WorkflowRunStatus.ERROR


def test_halted_state(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    """Tests that the workflow reaper successfully checks for halted state.

    Halted state occurs when a workflow run is either in C (cold resume) or
    H (hot resume) state. The reaper will then transition the workflow to S
    state, it will not transition the workflow_run.
    """
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    # Create first WorkflowRun and leave it in running state. log a heartbeat so it doesn't
    # get reaped
    task1 = sleepy_task_template.create_task(sleep=10)
    workflow1 = base_tool.create_workflow()
    workflow1.add_tasks([task1])
    workflow1.bind()
    wfr1 = WorkflowRun(workflow=workflow1, requester=workflow1.requester)
    wfr1._link_to_workflow(0)
    wfr1._log_heartbeat(300)
    wfr1._update_status(WorkflowRunStatus.BOUND)
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)

    # Create second WorkflowRun and transition to C status
    task2 = sleepy_task_template.create_task(sleep=11)
    workflow2 = base_tool.create_workflow(
        name="reaper_halted_test_2", workflow_args="halted_v_2"
    )

    workflow2.add_tasks([task2])
    workflow2.bind()
    wfr2 = WorkflowRun(workflow=workflow2, requester=workflow2.requester)
    wfr2._link_to_workflow(0)
    wfr2._update_status(WorkflowRunStatus.BOUND)
    wfr2._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr2._update_status(WorkflowRunStatus.LAUNCHED)
    wfr2._update_status(WorkflowRunStatus.RUNNING)
    wfr2._update_status(WorkflowRunStatus.COLD_RESUME)

    # Create third WorkflowRun and transition to H status
    task3 = sleepy_task_template.create_task(sleep=12)
    workflow3 = base_tool.create_workflow(
        name="reaper_halted_test", workflow_args="halted_v_1"
    )

    workflow3.add_tasks([task3])
    workflow3.bind()
    wfr3 = WorkflowRun(workflow=workflow3, requester=workflow3.requester)
    wfr3._link_to_workflow(0)
    wfr3._update_status(WorkflowRunStatus.BOUND)
    wfr3._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr3._update_status(WorkflowRunStatus.LAUNCHED)
    wfr3._update_status(WorkflowRunStatus.RUNNING)
    wfr3._update_status(WorkflowRunStatus.HOT_RESUME)

    def mock_slack_notifier(msg: str):
        pass

    # Call workflow reaper suspended state
    reaper = WorkflowReaper(
        5 * 60, requester=requester_no_retry, wf_notification_sink=mock_slack_notifier
    )
    msg = reaper._halted_state()
    assert (
        f"{__version__} Workflow Reaper transitioned a Workflow to HALTED state "
        f"and associated Workflow Run to TERMINATED state.\nWorkflow ID:"
        f" {workflow2.workflow_id}\nWorkflow Name: reaper_halted_test_2\nWorkflow "
        f"Args: halted_v_2\nWorkflowRun ID: {wfr2.workflow_run_id}{__version__} "
        f"Workflow Reaper transitioned a Workflow to HALTED state and associated "
        f"Workflow Run to TERMINATED state.\nWorkflow ID: {workflow3.workflow_id}\n"
        f"Workflow Name: reaper_halted_test\nWorkflow Args: halted_v_1\n"
        f"WorkflowRun ID: {wfr3.workflow_run_id}" in msg
    )

    # Check that the workflow runs are in the same state (1 R, 2 T)
    # and that there are two workflows in S state and one still in R state
    wfr1_status = get_workflow_run_status(db_cfg, wfr1.workflow_run_id)
    wfr2_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    wfr3_status = get_workflow_run_status(db_cfg, wfr3.workflow_run_id)
    assert wfr1_status == WorkflowRunStatus.RUNNING
    assert wfr2_status == WorkflowRunStatus.TERMINATED
    assert wfr3_status == WorkflowRunStatus.TERMINATED

    workflow1_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow2_status = get_workflow_status(db_cfg, workflow2.workflow_id)
    workflow3_status = get_workflow_status(db_cfg, workflow3.workflow_id)
    assert workflow1_status == WorkflowStatus.RUNNING
    assert workflow2_status == WorkflowStatus.HALTED
    assert workflow3_status == WorkflowStatus.HALTED


def test_aborted_state(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun

    # create a workflow without binding the tasks. log a heartbeat so it doesn't get reaped
    task = sleepy_task_template.create_task(sleep=10)
    task2 = sleepy_task_template.create_task(sleep=11)
    workflow1 = base_tool.create_workflow()
    workflow1.add_tasks([task, task2])
    workflow1.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr1 = WorkflowRun(workflow=workflow1, requester=requester_no_retry)
    wfr1._link_to_workflow(90)

    # create a workflow without binding the tasks
    workflow2 = base_tool.create_workflow(
        name="reaper_aborted_test", workflow_args="aborted_v_1"
    )
    workflow2.add_tasks([task, task2])
    workflow2.bind()
    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr2 = WorkflowRun(workflow=workflow2, requester=requester_no_retry)
    wfr2._link_to_workflow(0)

    # Call aborted state logic
    def mock_slack_notifier(msg: str):
        pass

    reaper = WorkflowReaper(
        5 * 60, requester=requester_no_retry, wf_notification_sink=mock_slack_notifier
    )
    msg = reaper._aborted_state()
    assert (
        f"{__version__} Workflow Reaper transitioned a Workflow to ABORTED state "
        f"and associated Workflow Run to ABORTED state.\nWorkflow ID: "
        f"{workflow2.workflow_id}\nWorkflow Name: reaper_aborted_test\nWorkflow Args:"
        f" aborted_v_1\nWorkflowRun ID: {wfr2.workflow_run_id}" in msg
    )

    # Check that the workflow_run and workflow have both been moved to the
    # "A" state.
    workflow_status = get_workflow_status(db_cfg, workflow1.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr1.workflow_run_id)
    assert workflow_run_status == WorkflowRunStatus.LINKING
    assert workflow_status == WorkflowStatus.REGISTERING

    workflow_status = get_workflow_status(db_cfg, workflow2.workflow_id)
    workflow_run_status = get_workflow_run_status(db_cfg, wfr2.workflow_run_id)
    assert workflow_run_status == WorkflowRunStatus.ABORTED
    assert workflow_status == WorkflowStatus.ABORTED


def test_reaper_version(db_cfg, requester_no_retry, base_tool, sleepy_task_template):
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.workflow_run import WorkflowRun

    # create a workflow without binding the tasks. log a heartbeat so it doesn't get reaped
    task = sleepy_task_template.create_task(sleep=10)
    task2 = sleepy_task_template.create_task(sleep=11)
    workflow = base_tool.create_workflow()
    workflow.add_tasks([task, task2])
    workflow.bind()

    # Re-implement the logic of _create_workflow_run.
    # This will allow us to keep the workflow_run in G state and not bind it
    wfr = WorkflowRun(workflow=workflow, requester=requester_no_retry)
    wfr._link_to_workflow(0)
    wfr._log_heartbeat(0)

    # Check for lost workflow runs
    reaper = WorkflowReaper(5, requester=requester_no_retry)
    reaper_wfrs = reaper._get_lost_workflow_runs([WorkflowRunStatus.LINKING])

    assert len(reaper_wfrs) > 0
    assert wfr.workflow_run_id in [wfr.workflow_run_id for wfr in reaper_wfrs]

    # Mock the version to some nonsense
    with patch.object(WorkflowReaper, "_version", new_callable=PropertyMock) as mock:
        mock.return_value = "foobar"

        statuses = [
            WorkflowRunStatus.LINKING,
            WorkflowRunStatus.COLD_RESUME,
            WorkflowRunStatus.HOT_RESUME,
        ]
        no_wfrs = reaper._get_lost_workflow_runs(statuses)
        assert len(no_wfrs) == 0


def test_inconsistent_status(db_cfg, client_env):
    """Tests that workflows with inconsistent F versus D status get repaired."""
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
    from jobmon.client.tool import Tool

    # setup workflow
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflows = []
    for i in range(4):
        workflows.append(_create_workflow_inconsistency_check(tool, i))
        workflows[i].run()

    # Force the first two to be inconsistent
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # fake workflow run
        DB.session.execute(
            f"""
            UPDATE workflow
            SET status ='F'
            WHERE id in ({workflows[0].workflow_id}, {workflows[1].workflow_id})"""
        )
        DB.session.commit()

    # make sure it starts with 0
    # Force it to check
    # Check a large window because pervious tests will leave workflows in the database
    WorkflowReaper._current_starting_row = 0
    WorkflowReaper(
        poll_interval_seconds=5 * 60, requester=workflows[0].requester
    )._inconsistent_status(200)
    assert WorkflowReaper._current_starting_row == 0

    # check workflow status changed on both
    with app.app_context():
        # fake workflow run
        s = DB.session.execute(
            f"""
            select status
            FROM workflow
            WHERE id={workflows[0].workflow_id}"""
        ).fetchone()["status"]
        assert s == "D"
        s = DB.session.execute(
            f"""
            select status
            FROM workflow
            WHERE id={workflows[1].workflow_id}"""
        ).fetchone()["status"]
        assert s == "D"

    # Now check that the workflow_id wraps
    WorkflowReaper(
        poll_interval_seconds=5 * 60, requester=workflows[0].requester
    )._inconsistent_status(1)
    assert WorkflowReaper._current_starting_row == 1
    WorkflowReaper(
        poll_interval_seconds=5 * 60, requester=workflows[0].requester
    )._inconsistent_status(1000)
    assert WorkflowReaper._current_starting_row == 0


def _create_workflow_inconsistency_check(tool, wf_id: int):
    task_template = tool.get_task_template(
        template_name=f"test_inconsistent_status",
        command_template="sleep 10 && echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    workflow = tool.create_workflow(name=f"test_inconsistent_status_{wf_id}")
    task_a = task_template.create_task(arg="1")
    workflow.add_task(task_a)
    task_b = task_template.create_task(arg="2")
    workflow.add_task(task_b)
    return workflow
