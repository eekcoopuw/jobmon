import pytest

from jobmon.requester import Requester
from jobmon.constants import WorkflowRunStatus, WorkflowStatus


@pytest.fixture
def tool(db_cfg, client_env):
    from jobmon.client.tool import Tool
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


def test_instantiating_launched(db_cfg, tool, task_template):
    """Check that the workflow and WFR are moving through appropriate states"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor

    workflow = tool.create_workflow(name="test_instantiated_launched")

    t1 = task_template.create_task(arg="echo 1")
    workflow.add_task(t1)

    # Bind, and check state
    workflow.bind()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT status
        FROM workflow
        WHERE id = :workflow_id"""
        res = DB.session.execute(sql, {"workflow_id": workflow.workflow_id}).fetchone()
        DB.session.commit()
    assert res[0] == WorkflowStatus.REGISTERING

    # Create workflow run
    wfr = workflow._create_workflow_run()

    # Workflow should be queued, wfr should be bound
    workflow_status_sql = """
        SELECT w.status, wr.status
        FROM workflow_run wr
        JOIN workflow w on w.id = wr.workflow_id
        WHERE wr.id = :workflow_run_id"""
    with app.app_context():
        res = DB.session.execute(
            workflow_status_sql, {"workflow_run_id": wfr.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res == (WorkflowStatus.QUEUED, WorkflowRunStatus.BOUND)

    # Force some invalid state transitions
    # Don't waste time on retries, redefine requester
    req = Requester(workflow.requester.url, 1, 5)
    wfr.requester = req

    with pytest.raises(RuntimeError):
        # Needs to be instantiating first
        wfr._update_status(WorkflowRunStatus.LAUNCHED)

    with pytest.raises(RuntimeError):
        # Needs to be launched first
        wfr._update_status(WorkflowRunStatus.RUNNING)

    # Something failed and the wfr errors out.
    wfr._update_status(WorkflowRunStatus.ERROR)

    with app.app_context():
        res = DB.session.execute(
            workflow_status_sql, {"workflow_run_id": wfr.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res == (WorkflowStatus.FAILED, WorkflowRunStatus.ERROR)

    # Create a new workflow run, and resume
    wfr2 = workflow._create_workflow_run(resume=True)

    with app.app_context():
        res = DB.session.execute(
            workflow_status_sql, {"workflow_run_id": wfr2.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res == (WorkflowStatus.QUEUED, WorkflowRunStatus.BOUND)

    # Start the distributor
    DistributorService(
        workflow.workflow_id,
        wfr2.workflow_run_id,
        SequentialDistributor(),
        requester=workflow.requester,
    )
    with app.app_context():
        res = DB.session.execute(
            workflow_status_sql, {"workflow_run_id": wfr2.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res == (WorkflowStatus.RUNNING, WorkflowRunStatus.RUNNING)
