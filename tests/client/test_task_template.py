import uuid

import pytest


@pytest.fixture(scope='function')
def tool(client_env):
    from jobmon.client.tool import Tool
    return Tool.create_tool(name=str(uuid.uuid4()))


def test_task_template(db_cfg, client_env, tool):
    from jobmon.client.task_template import TaskTemplate
    from jobmon.client.task_template_version import TaskTemplateVersion
    tt = TaskTemplate("my_template")
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

    tool.create_new_tool_version()
    tt.bind(tool.tool_version_id)
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
        op_args=["op1"])
    assert tt1.task_template_id

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"])

    assert tt1.task_template_version.id == tt2.task_template_version.id

    # test duplicate insert
    resp = tt2.task_template_version._insert_task_template_version()
    assert tt1.task_template_version.id == resp[0]


def test_create_new_task_template_version(db_cfg, client_env, tool):
    """test that a new task template version gets created when the arguments
    that define it change. confirm that reused arguments have the same id"""
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"])
    assert tt1.task_template_id

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1} --bar {task2}",
        node_args=["node1"],
        task_args=["task1", "task2"],
        op_args=["op1"])

    assert tt1.task_template_id == tt2.task_template_id
    assert tt1.task_template_version.id != tt2.task_template_version.id
    assert (
        tt1.task_template_version.id_name_map["node1"] ==
        tt2.task_template_version.id_name_map["node1"])


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
