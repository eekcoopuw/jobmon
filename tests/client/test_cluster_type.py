# from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.client.tool import Tool
from jobmon.client.workflow_run import WorkflowRun
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (
    WorkflowAlreadyComplete,
    DuplicateNodeArgsError,
    WorkflowAlreadyExists,
    NodeDependencyNotExistError,
    WorkflowNotResumable,
)

import pytest


@pytest.fixture
def tool(db_cfg, client_env):
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


def test_get_cluster_type_id_by_task_id(db_cfg, tool, client_env, task_template):
    workflow = tool.create_workflow(name="numpy_test_wf")
    template = tool.get_task_template(
        template_name="test_get_cluster_type_id_by_task_id",
        command_template="echo {node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
    )
    task = template.create_task(node_arg={1, 2}, task_arg={3, 4})
    workflow.add_tasks([task])
    workflow.bind()
    workflow.bind_arrays()
    wfr = workflow._create_workflow_run()

    # Get cluster_type_id by task_id
    app_route = f"/cluster_type/task_id/{task.task_id}"
    return_code, resp = workflow.requester.send_request(
        app_route=app_route,
        message={},
        request_type="get",
    )
    assert return_code == 200
    ctid_from_restful = resp["cluster_type_id"]
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """select id 
                 from cluster_type 
                 where name="sequential"
            """
        row = DB.session.execute(sql).fetchone()
        assert row["id"] == ctid_from_restful
