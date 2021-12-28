import pytest

from jobmon.constants import WorkflowRunStatus, WorkflowStatus


@pytest.fixture
def tool(db_cfg, client_env):
    from jobmon.client.api import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


def test_log_heartbeat(tool, task_template, db_cfg):
    """test _log_heartbeat sets the wfr status to L"""
    from jobmon.client.workflow_run import WorkflowRun

    wf = tool.create_workflow()
    t1 = task_template.create_task(arg="sleep 1")
    wf.add_tasks([t1])
    wf.bind()
    wfr = WorkflowRun(workflow_id=wf._workflow_id, requester=wf.requester)
    id, s = wfr._link_to_workflow(89)
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING
    # get current heartbeat
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = "SELECT heartbeat_date " "FROM workflow_run " "WHERE id={} ".format(id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    current_hb = res[0]
    wfr._log_heartbeat(90)
    with app.app_context():
        query = "SELECT heartbeat_date " "FROM workflow_run " "WHERE id={} ".format(id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    new_hb = res[0]
    assert new_hb > current_hb
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING
