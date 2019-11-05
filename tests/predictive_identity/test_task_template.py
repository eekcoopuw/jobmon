import pytest

from jobmon.client.swarm.workflow.tool import Tool


@pytest.fixture(scope='function')
def tool():
    return Tool.create_tool(name="foo")


def test_create_and_get_task_template(db_cfg, env_var, tool):
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"])
    assert tt1._template_version_created is True

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"])

    assert tt1.task_template_version.id == tt2.task_template_version.id


def test_create_new_task_template_version(db_cfg, env_var, tool):
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"])
    assert tt1._template_version_created is True

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


def test_invalid_args(db_cfg, env_var, tool):

    with pytest.raises(ValueError):
        tool.get_task_template(
            template_name="my_template",
            command_template="{op1} {node1} --foo {task1}",
            node_args=["node1"],
            task_args=["task2"],
            op_args=["op1"])
