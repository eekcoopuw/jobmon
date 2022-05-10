import os
import pytest
import uuid


@pytest.fixture(scope="function")
def tool(client_env):
    from jobmon.client.tool import Tool

    return Tool(name=str(uuid.uuid4()))


def test_task_template(db_cfg, client_env, tool):
    from jobmon.client.task_template import TaskTemplate
    from jobmon.client.task_template_version import TaskTemplateVersion

    tool.get_new_tool_version()

    tt = TaskTemplate("my_template")
    tt.bind(tool.active_tool_version)
    tt.get_task_template_version(
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"],
    )
    assert tt.active_task_template_version

    # make sure both methods get same result
    ttv = TaskTemplateVersion(
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"],
    )
    tt.set_active_task_template_version(ttv)
    assert len(tt.task_template_versions) == 1

    tt.active_task_template_version.bind(tt)
    ttv.bind(tt)
    assert tt.active_task_template_version.id == ttv.id


def test_create_and_get_task_template(db_cfg, client_env, tool):
    """test that a task template gets added to the db appropriately. test that
    if a new one gets created with the same params it has the same id"""
    tt1 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"],
    )
    assert tt1.id

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1}",
        node_args=["node1"],
        task_args=["task1"],
        op_args=["op1"],
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
        op_args=["op1"],
    )
    assert tt1.id
    ttv1_id = tt1.active_task_template_version.id
    arg_id1 = tt1.active_task_template_version.id_name_map["node1"]

    tt2 = tool.get_task_template(
        template_name="my_template",
        command_template="{op1} {node1} --foo {task1} --bar {task2}",
        node_args=["node1"],
        task_args=["task1", "task2"],
        op_args=["op1"],
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
            op_args=["op1"],
        )


def test_task_template_resources(db_cfg, client_env, tool):
    """Test task/task template compute resources hierarchy."""

    workflow1 = tool.create_workflow(name="test_template_resources")
    tt_resources = {"queue": "null.q", "cores": 1, "max_runtime_seconds": 3}
    task_template = tool.get_task_template(
        template_name="random_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources=tt_resources,
    )
    task_resources = {"queue": "null.q", "cores": 1, "max_runtime_seconds": 2}
    task1 = task_template.create_task(
        arg="sleep 1", cluster_name="sequential", compute_resources=task_resources
    )
    task2 = task_template.create_task(arg="sleep 2", cluster_name="sequential")
    task3 = task_template.create_task(arg="sleep 3")
    workflow1.add_tasks([task1, task2, task3])
    workflow1.bind()
    workflow1._create_workflow_run()

    assert task1.original_task_resources.requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 2,
    }
    assert task1.original_task_resources.queue.queue_name == "null.q"
    assert task2.original_task_resources.requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 3,
    }
    assert task2.original_task_resources.queue.queue_name == "null.q"
    assert task3.original_task_resources.requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 3,
    }
    assert task3.original_task_resources.queue.queue_name == "null.q"


def test_task_template_resources_yaml(client_env, db_cfg, tool):
    """Test users ability to set task template compute resources via YAML."""
    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
    yaml_file = os.path.join(thisdir, "cluster_resources.yaml")

    # Set first task template compute resources at initialization from YAML
    tt_1 = tool.get_task_template(
        template_name="preprocess",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        yaml_file=yaml_file,
    )

    # Set second task template compute resources during set_yaml method. Setting default
    # cluter name in get_task_template()
    tt_2 = tool.get_task_template(
        template_name="model",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
    )
    tt_2.set_default_compute_resources_from_yaml(yaml_file=yaml_file)

    # Same as tt_2, but testing when the default cluster name is set in
    # set_default_compute_resources_from_yaml()
    tt_3 = tool.get_task_template(
        template_name="model",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tt_3.set_default_compute_resources_from_yaml(
        default_cluster_name="sequential", yaml_file=yaml_file
    )

    assert tt_1.default_compute_resources_set["sequential"] == {
        "num_cores": 1,
        "m_mem_free": "3G",
        "max_runtime_seconds": "(60 * 60 * 4)",
        "queue": "null.q",
    }
    assert tt_2.default_compute_resources_set["sequential"] == {
        "num_cores": 3,
        "m_mem_free": "2G",
        "max_runtime_seconds": "(60 * 60 * 24)",
        "queue": "null.q",
    }
    assert tt_3.default_compute_resources_set["sequential"] == {
        "num_cores": 3,
        "m_mem_free": "2G",
        "max_runtime_seconds": "(60 * 60 * 24)",
        "queue": "null.q",
    }
