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
    tt.bind(tool.active_tool_version.id)
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
    from jobmon.client.workflow_run import WorkflowRun

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
    client_wfr = WorkflowRun(workflow1.workflow_id)
    client_wfr.bind(workflow1.tasks)

    assert task1.task_resources._requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 2,
        "queue": "null.q",
    }
    assert task2.task_resources._requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 3,
        "queue": "null.q",
    }
    assert task3.task_resources._requested_resources == {
        "cores": 1,
        "max_runtime_seconds": 3,
        "queue": "null.q",
    }


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


def test_tt_resource_usage(db_cfg, client_env):
    """Test TaskTemplate resource usage method."""
    import numpy as np

    from jobmon.client.cli import ClientCLI as CLI
    from jobmon.client.status_commands import task_template_resources
    from jobmon.client.tool import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow_1 = tool.create_workflow(name="task_template_resource_usage_test_wf_1")
    workflow_2 = tool.create_workflow(name="task_template_resource_usage_test_wf_2")
    template = tool.get_task_template(
        template_name="task_template_resource_usage",
        command_template="echo {arg} --foo {arg_2} --bar {task_arg_1} --baz {arg_3}",
        node_args=["arg", "arg_2", "arg_3"],
        task_args=["task_arg_1"],
        op_args=[],
    )
    template_2 = tool.get_task_template(
        template_name="task_template_resource_usage_2",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    task_1 = template.create_task(
        arg="Acadia",
        arg_2="DeathValley",
        task_arg_1="NorthCascades",
        arg_3="Yellowstone",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_2 = template.create_task(
        arg="Zion",
        arg_2="JoshuaTree",
        task_arg_1="Olympic",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )
    task_3 = template.create_task(
        arg="Rainier",
        arg_2="Badlands",
        task_arg_1="CraterLake",
        arg_3="GrandTeton",
        compute_resources={"max_runtime_seconds": 30},
    )

    workflow_1.add_tasks([task_1, task_2])
    workflow_1.run()
    workflow_2.add_tasks([task_3])
    workflow_2.run()

    # Add fake resource usage to the TaskInstances
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query_1 = """
        UPDATE task_instance
        SET wallclock = 10, maxpss = 300
        WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})

        query_2 = """
        UPDATE task_instance
        SET wallclock = 20, maxpss = 600
        WHERE task_id = :task_id"""
        DB.session.execute(query_2, {"task_id": task_2.task_id})

        query_3 = """
        UPDATE task_instance
        SET wallclock = 30, maxpss = 900
        WHERE task_id = :task_id"""
        DB.session.execute(query_3, {"task_id": task_3.task_id})
        DB.session.commit()

    # Check the aggregate resources for all workflows
    used_task_template_resources = template.resource_usage(ci=0.95)

    resources = {
        "num_tasks": 3,
        "min_mem": "300B",
        "max_mem": "900B",
        "mean_mem": "600.0B",
        "min_runtime": 10,
        "max_runtime": 30,
        "mean_runtime": 20.0,
        "median_mem": "20.0B",
        "median_runtime": 600.0,
        "ci_mem": [-145.24, 1345.24],
        "ci_runtime": [-4.84, 44.84],
    }
    assert used_task_template_resources == resources

    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id}"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version
    )
    assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
    assert used_task_template_resources["min_mem"] == resources["min_mem"]
    assert used_task_template_resources["max_mem"] == resources["max_mem"]
    assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
    assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
    assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
    assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
    assert used_task_template_resources["median_mem"] == resources["median_mem"]
    assert used_task_template_resources["median_runtime"] == resources["median_runtime"]
    assert np.isnan(used_task_template_resources["ci_mem"][0])
    assert np.isnan(used_task_template_resources["ci_mem"][1])
    assert np.isnan(used_task_template_resources["ci_runtime"][0])
    assert np.isnan(used_task_template_resources["ci_runtime"][1])

    # Check the aggregate resources for the first workflow
    used_task_template_resources = template.resource_usage(
        workflows=[workflow_1.workflow_id], ci=0.95
    )
    resources = {
        "num_tasks": 2,
        "min_mem": "300B",
        "max_mem": "600B",
        "mean_mem": "450.0B",
        "min_runtime": 10,
        "max_runtime": 20,
        "mean_runtime": 15.0,
        "median_mem": "15.0B",
        "median_runtime": 450.0,
        "ci_mem": [-1455.93, 2355.93],
        "ci_runtime": [-48.53, 78.53],
    }
    assert used_task_template_resources == resources

    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id} -w"
        f" {workflow_1.workflow_id}"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version,
        workflows=args.workflows,
        ci=0.95,
    )
    assert used_task_template_resources == resources

    # Check the aggregate resources for the first and second workflows
    used_task_template_resources = template.resource_usage(
        workflows=[workflow_1.workflow_id, workflow_2.workflow_id], ci=0.95
    )
    resources = {
        "num_tasks": 3,
        "min_mem": "300B",
        "max_mem": "900B",
        "mean_mem": "600.0B",
        "min_runtime": 10,
        "max_runtime": 30,
        "mean_runtime": 20.0,
        "median_mem": "20.0B",
        "median_runtime": 600.0,
        "ci_mem": [-145.24, 1345.24],
        "ci_runtime": [-4.84, 44.84],
    }
    assert used_task_template_resources == resources

    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id} "
        f"-w {workflow_1.workflow_id} {workflow_2.workflow_id}"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version,
        workflows=args.workflows,
        ci=0.95,
    )
    assert used_task_template_resources == resources

    # Check the outcome of resource_usage of a task_template that has no Tasks
    used_task_template_resources = template_2.resource_usage(ci=0.95)
    resources = {
        "num_tasks": None,
        "min_mem": None,
        "max_mem": None,
        "mean_mem": None,
        "min_runtime": None,
        "max_runtime": None,
        "mean_runtime": None,
        "median_mem": None,
        "median_runtime": None,
        "ci_mem": None,
        "ci_runtime": None,
    }
    assert used_task_template_resources == resources

    command_str = (
        f"task_template_resources -t {template_2._active_task_template_version.id}"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version, ci=0.95
    )
    assert used_task_template_resources == resources

    # Check the aggregate resources when two node args of same type are passed in (tasks 1 & 3)
    used_task_template_resources = template.resource_usage(
        node_args={"arg": ["Acadia", "Rainier"]}, ci=0.95
    )
    resources = {
        "num_tasks": 2,
        "min_mem": "300B",
        "max_mem": "900B",
        "mean_mem": "600.0B",
        "min_runtime": 10,
        "max_runtime": 30,
        "mean_runtime": 20.0,
        "median_mem": "20.0B",
        "median_runtime": 600.0,
        "ci_mem": [-3211.86, 4411.86],
        "ci_runtime": [-107.06, 147.06],
    }
    assert used_task_template_resources == resources

    node_args = '{"arg":["Acadia","Rainier"]}'
    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id} -a"
        f" '{node_args}'"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version,
        node_args=args.node_args,
        ci=0.95,
    )
    assert used_task_template_resources == resources

    # Check the aggregate resources when one node arg of two types are passed in (tasks 2 & 3)
    used_task_template_resources = template.resource_usage(
        node_args={"arg": ["Zion"], "arg_2": ["Badlands"]}, ci=0.99
    )
    resources = {
        "num_tasks": 2,
        "min_mem": "600B",
        "max_mem": "900B",
        "mean_mem": "750.0B",
        "min_runtime": 20,
        "max_runtime": 30,
        "mean_runtime": 25.0,
        "median_mem": "25.0B",
        "median_runtime": 750.0,
        "ci_mem": [-8798.51, 10298.51],
        "ci_runtime": [-293.28, 343.28],
    }
    assert used_task_template_resources == resources

    node_args = '{"arg": ["Zion"], "arg_2": ["Badlands"]}'
    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id} -a"
        f" '{node_args}'"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version,
        node_args=args.node_args,
        ci=0.99,
    )
    assert used_task_template_resources == resources

    # Check the aggregate resources when node args and workflow ids are passed in (Task 3)
    used_task_template_resources = template.resource_usage(
        node_args={"arg_3": ["GrandTeton"]}, workflows=[workflow_2.workflow_id]
    )

    resources = {
        "num_tasks": 1,
        "min_mem": "900B",
        "max_mem": "900B",
        "mean_mem": "900.0B",
        "min_runtime": 30,
        "max_runtime": 30,
        "mean_runtime": 30.0,
        "median_mem": "30.0B",
        "median_runtime": 900.0,
    }
    assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
    assert used_task_template_resources["min_mem"] == resources["min_mem"]
    assert used_task_template_resources["max_mem"] == resources["max_mem"]
    assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
    assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
    assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
    assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
    assert used_task_template_resources["median_mem"] == resources["median_mem"]
    assert used_task_template_resources["median_runtime"] == resources["median_runtime"]
    assert np.isnan(used_task_template_resources["ci_mem"][0])
    assert np.isnan(used_task_template_resources["ci_mem"][1])
    assert np.isnan(used_task_template_resources["ci_runtime"][0])
    assert np.isnan(used_task_template_resources["ci_runtime"][1])

    node_args = '{"arg_3":["GrandTeton"]}'
    command_str = (
        f"task_template_resources -t {template._active_task_template_version.id} -a"
        f" '{node_args}' -w {workflow_2.workflow_id}"
    )
    cli = CLI()
    args = cli.parse_args(command_str)
    used_task_template_resources = task_template_resources(
        task_template_version=args.task_template_version,
        node_args=args.node_args,
        workflows=args.workflows,
    )
    assert used_task_template_resources["num_tasks"] == resources["num_tasks"]
    assert used_task_template_resources["min_mem"] == resources["min_mem"]
    assert used_task_template_resources["max_mem"] == resources["max_mem"]
    assert used_task_template_resources["mean_mem"] == resources["mean_mem"]
    assert used_task_template_resources["min_runtime"] == resources["min_runtime"]
    assert used_task_template_resources["max_runtime"] == resources["max_runtime"]
    assert used_task_template_resources["mean_runtime"] == resources["mean_runtime"]
    assert used_task_template_resources["median_mem"] == resources["median_mem"]
    assert used_task_template_resources["median_runtime"] == resources["median_runtime"]
    assert np.isnan(used_task_template_resources["ci_mem"][0])
    assert np.isnan(used_task_template_resources["ci_mem"][1])
    assert np.isnan(used_task_template_resources["ci_runtime"][0])
    assert np.isnan(used_task_template_resources["ci_runtime"][1])


def test_max_mem(db_cfg, client_env):
    from jobmon.client.tool import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow_1 = tool.create_workflow(name="task_template_mem_test")
    template = tool.get_task_template(
        template_name="task_template_resource_usage",
        command_template="echo {arg} --foolili {arg_2} --bar {task_arg_1} --baz {arg_3}",
        node_args=["arg", "arg_2", "arg_3"],
        task_args=["task_arg_1"],
        op_args=[],
    )
    task_1 = template.create_task(
        arg="Acadia",
        arg_2="DeathValley",
        task_arg_1="NorthCascades",
        arg_3="Yellowstone",
        compute_resources={"max_runtime_seconds": 30},
    )

    workflow_1.add_tasks([task_1])
    workflow_1.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]

    # return 0 when both null
    with app.app_context():
        query_1 = """
            UPDATE task_instance
            SET maxpss = null, maxrss=null
            WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "0B"

    # return the other when 1 is null
    with app.app_context():
        query_1 = """
                UPDATE task_instance
                SET maxpss = 1, maxrss=null
                WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "1B"

    with app.app_context():
        query_1 = """
                UPDATE task_instance
                SET maxpss = null, maxrss=1
                WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "1B"

    # return the bigger one when both has value
    with app.app_context():
        query_1 = """
                UPDATE task_instance
                SET maxpss = -1, maxrss=1
                WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "1B"
    with app.app_context():
        query_1 = """
                UPDATE task_instance
                SET maxpss = 1, maxrss= -1
                WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "1B"

    # return 0 when both -1
    with app.app_context():
        query_1 = """
                UPDATE task_instance
                SET maxpss = -1, maxrss= -1
                WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": task_1.task_id})
        DB.session.commit()
    resources = template.resource_usage()
    assert resources["max_mem"] == "0B"
