import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy import update

from jobmon.constants import WorkflowRunStatus, TaskStatus, TaskInstanceStatus
from jobmon.client.task import Task
from jobmon.client.workflow_run import WorkflowRun
from jobmon.exceptions import InvalidResponse
from jobmon.server.web.models import load_model
from jobmon.server.web.models import task
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.workflow import Workflow

load_model()

def test_get_task_template_version(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )
    tt2 = t.get_task_template(
        template_name="tt2",
        command_template="echo {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    task_2 = tt1.create_task(arg=2)
    task_3 = tt2.create_task(arg=3)
    wf.add_tasks([task_1, task_2, task_3])
    wf.bind()
    wf._create_workflow_run()

    # Test getting task template for task
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"task_id": task_1.task_id}, request_type="get"
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'bash_task'}]}
    assert len(msg) == 1
    assert "task_template_version_ids" in msg.keys()
    assert len(msg["task_template_version_ids"]) == 1
    assert "id" in msg["task_template_version_ids"][0].keys()
    assert msg["task_template_version_ids"][0]["name"] == "tt1"

    # Test getting task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"workflow_id": wf.workflow_id}, request_type="get"
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'tt1'}, {'id': 2, 'name': 'tt2'}]}
    assert len(msg) == 1
    assert "task_template_version_ids" in msg.keys()
    assert len(msg["task_template_version_ids"]) == 2
    for i in msg["task_template_version_ids"]:
        if i["id"] == tt1._active_task_template_version.id:
            assert i["name"] == "tt1"
        else:
            assert i["name"] == "tt2"


def test_get_requested_cores(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._create_workflow_run()

    # Get task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"workflow_id": wf.workflow_id}, request_type="get"
    )
    ttvis = msg["task_template_version_ids"][0]["id"]
    # Test getting requested cores
    app_route = "/get_requested_cores"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_template_version_ids": f"({ttvis})"},
        request_type="get",
    )
    # msg = {'core_info': [{'avg': 2, 'id': 1, 'max': 3, 'min': 1}]}
    assert len(msg["core_info"]) == 1
    assert msg["core_info"][0]["id"] == ttvis
    assert msg["core_info"][0]["min"] == 2
    assert msg["core_info"][0]["max"] == 4
    assert msg["core_info"][0]["avg"] == 3


@pytest.mark.skip(reason="wf.run seems not working; revisit later")
def test_most_popular_queue(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_q_1", command_template="echo {arg}", node_args=["arg"]
    )
    tt2 = t.get_task_template(
        template_name="tt_q_2", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t2 = tt1.create_task(
        arg=2, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t3 = tt2.create_task(
        arg=3, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t4 = tt2.create_task(
        arg=4, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t5 = tt2.create_task(
        arg=5, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf.add_tasks([t1, t2, t3, t4, t5])
    wf.bind()
    wf.run()

    app_route = "/get_most_popular_queue"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_ids": f"({tt1._active_task_template_version.id}, "
            f"{tt2._active_task_template_version.id})"
        },
        request_type="get",
    )
    # msg = {'queue_info': [{'id': 1, 'queue': 'all.q'}, {'id': 2, 'queue': 'long.q'}]}
    assert len(msg["queue_info"]) == 2
    for i in msg["queue_info"]:
        assert i["queue"] == "null.q"


def test_get_workflow_validation_status(db_engine, tool):
    t = tool
    wf1 = t.create_workflow(name="i_am_a_fake_wf")
    wf2 = t.create_workflow(name="i_am_another_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf1.add_tasks([t1])
    wf1.bind()
    wf1._create_workflow_run()
    wf2.add_tasks([t2])
    wf2.bind()
    wf2._create_workflow_run()

    app_route = "/workflow_validation"
    return_code, msg = wf1.requester.send_request(
        app_route=app_route,
        message={
            "task_ids": [t1.task_id, t2.task_id]
        },
        request_type="post",
    )
    assert return_code == 200
    assert msg["validation"] is False

