import pytest

from sqlalchemy.orm import Session

from jobmon.constants import WorkflowRunStatus, WorkflowStatus
from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRun


@pytest.fixture
def tool(client_env):

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


def test_log_heartbeat(tool, task_template, db_engine):
    """test _log_heartbeat sets the wfr status to L"""

    wf = tool.create_workflow()
    t1 = task_template.create_task(arg="sleep 1")
    wf.add_tasks([t1])
    wf.bind()
    wfr = WorkflowRun(workflow=wf, requester=wf.requester)
    id, s = wfr._link_to_workflow(89)
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING
    # get current heartbeat
    # Validate that the database indicates the Dag and its Jobs are complete
    with Session(bind=db_engine) as session:
        query = "SELECT heartbeat_date " "FROM workflow_run " "WHERE id={} ".format(id)
        res = session.execute(query).fetchone()
        session.commit()
    current_hb = res[0]
    wfr._log_heartbeat(90)

    with Session(bind=db_engine) as session:
        query = "SELECT heartbeat_date " "FROM workflow_run " "WHERE id={} ".format(id)
        res = session.execute(query).fetchone()
        session.commit()
    new_hb = res[0]
    assert new_hb > current_hb
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING


def test_task_resources_conversion(tool, task_template):
    too_many_cores = {"memory": "20G", "queue": "null.q", "runtime": "01:02:33"}
    t1 = task_template.create_task(
        arg="echo 1", compute_resources=too_many_cores, cluster_name="multiprocess"
    )
    wf1 = tool.create_workflow()
    wf1.add_task(t1)

    # Check the workflow can still bind
    wf1.bind()
    wfr = wf1._create_workflow_run()
    task_resources = list(wfr._task_resources.values())[0]
    assert task_resources.requested_resources["memory"] == 20
    assert task_resources.queue.queue_name == "null.q"
    assert task_resources.requested_resources["runtime"] == 3753

    assert wfr.status == "B"
