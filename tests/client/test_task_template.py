import uuid

import pytest

from jobmon.client.task import Task
from jobmon.client.tool import Tool

@pytest.fixture(scope='function')
def tool(client_env):
    from jobmon.client.tool import Tool
    return Tool(name=str(uuid.uuid4()))


def test_task_template(db_cfg, client_env, tool):
    from jobmon.client.task_template import TaskTemplate
    from jobmon.client.task_template_version import TaskTemplateVersion
    tool.get_new_tool_version()

    tt = TaskTemplate("my_template")
    tt.bind(tool.active_tool_version.id)
    tt.get_task_template_version(
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"]
    )
    assert tt.active_task_template_version

    # make sure both methods get same result
    ttv = TaskTemplateVersion(
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"]
    )
    tt.set_active_task_template_version(ttv)
    assert len(tt.task_template_versions) == 1

    tt.active_task_template_version.bind(tt.id)
    ttv.bind(tt.id)
    assert tt.active_task_template_version.id == ttv.id


def test_create_and_get_task_template(db_cfg, client_env, tool):
    """test that a task template gets added to the db appropriately. test that
    if a new one gets created with the same params it has the same id"""
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"]
    )
    assert tt1.id

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"]
    )

    assert tt1.active_task_template_version.id == tt2.active_task_template_version.id


def test_create_new_task_template_version(db_cfg, client_env, tool):
    """test that a new task template version gets created when the arguments
    that define it change. confirm that reused arguments have the same id"""
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"]
    )
    assert tt1.id
    ttv1_id = tt1.active_task_template_version.id
    arg_id1 = tt1.active_task_template_version.id_name_map["node1"]

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1} --bar {task2}",
        node_args=["node1"],
        task_args=["task1", "task2"],
        op_args=["op1"]
    )
    ttv2_id = tt2.active_task_template_version.id
    arg_id2 = tt2.active_task_template_version.id_name_map["node1"]

    assert tt1.id == tt2.id
    assert ttv1_id != ttv2_id
    assert arg_id1 == arg_id2


def test_invalid_args(db_cfg, client_env, tool):
    """test that arguments that don't appear in the command template raise a
    ValueError"""

    with pytest.raises(ValueError):
        tool.get_task_template(
            template_name="my_template",
            command_template="{op1} {node1} --foo {task1}",
            node_args=["node1"],
            task_args=["task2"],
            op_args=["op1"])


def test_task_template_resources(db_cfg, client_env, tool):
    """Test task/task template compute resources hierarchy."""
    from jobmon.client.workflow_run import WorkflowRun

    workflow1 = tool.create_workflow(name="test_template_resources")
    tt_resources = {"sequential": {"queue": "null.q", "cores": 1, "max_runtime_seconds": 3}}
    task_template = tool.get_task_template(template_name="random_template",
                                           command_template="{arg}",
                                           node_args=["arg"],
                                           task_args=[],
                                           op_args=[],
                                           compute_resources=tt_resources)
    task_resources = {"queue": "null.q", "cores": 1, "max_runtime_seconds": 2}
    task1 = task_template.create_task(
        arg="sleep 1",
        cluster_name="sequential",
        compute_resources=task_resources
    )
    task2 = task_template.create_task(
        arg="sleep 2",
        cluster_name="sequential"
    )
    workflow1.add_tasks([task1, task2])
    workflow1.bind()
    client_wfr = WorkflowRun(workflow1.workflow_id)
    client_wfr.bind(workflow1.tasks)

    assert task1.task_resources._requested_resources == {'cores': 1, 'max_runtime_seconds': 2}
    assert task2.task_resources._requested_resources == {'cores': 1, 'max_runtime_seconds': 3}
