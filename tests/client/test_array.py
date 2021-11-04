import pytest

from jobmon.client.array import Array
from jobmon.client.tool import Tool


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
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


def test_create_array(db_cfg, client_env, task_template):

    array = task_template.create_array(arg="echo 1")
    assert (
        array.default_compute_resources_set
        == task_template.default_compute_resources_set["sequential"]
    )


def test_array_bind(db_cfg, client_env, task_template, tool):

    array = task_template.create_array(arg="echo 10")
    wf = tool.create_workflow()

    wf.add_array(array)
    wf.bind()
    wf.bind_arrays()

    assert hasattr(array, "_array_id")

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        array_stmt = """
        SELECT workflow_id, task_template_version_id 
        FROM array
        WHERE id = {}
        """.format(
            array.array_id
        )
        array_db = DB.session.execute(array_stmt).fetchone()
        DB.session.commit()

        assert array_db.workflow_id == wf.workflow_id
        assert (
            array_db.task_template_version_id
            == task_template.active_task_template_version.id
        )
    # Assert that array.bind is idempotent
    array_id = array.array_id
    wf.bind_arrays()

    assert array.array_id == array_id

    # Assert the bound task has the correct array ID
    wf._create_workflow_run()
    with app.app_context():
        task_query = """
        SELECT array_id
        FROM task
        WHERE id = {}
        """.format(
            array.tasks[0].task_id
        )
        task = DB.session.execute(task_query).fetchone()
        DB.session.commit()

        assert task.array_id == array.array_id


def test_node_args_expansion():

    node_args = {"location_id": [1, 2, 3], "sex": ["m", "f"]}

    expected_expansion = [
        {"location_id": 1, "sex": "m"},
        {"location_id": 1, "sex": "f"},
        {"location_id": 2, "sex": "m"},
        {"location_id": 2, "sex": "f"},
        {"location_id": 3, "sex": "m"},
        {"location_id": 3, "sex": "f"},
    ]

    node_arg_generator = Array.expand_dict(**node_args)
    combos = []
    for node_arg in node_arg_generator:
        assert node_arg in expected_expansion
        assert node_arg not in combos
        combos.append(node_arg)

    assert len(combos) == len(expected_expansion)


def test_create_tasks(db_cfg, client_env, tool):

    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{command} {task_arg} {narg1} {narg2} {op_arg}",
        node_args=["narg1", "narg2"],
        task_args=["task_arg"],
        op_args=["command", "op_arg"],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    array = rich_task_template.create_array(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        narg2=["a", "b", "c"],
        op_arg="baz",
    )

    assert len(array.tasks) == 9  # Created on init
    wf = tool.create_workflow()
    wf.add_array(array)

    assert len(wf.tasks) == 9  # Tasks bound to workflow

    # Assert tasks templated correctly
    commands = [t.command for t in array.tasks]
    assert "foo bar 1 c baz" in commands
    assert "foo bar 3 a baz" in commands

    # Check node and task args are recorded in the proper tables
    wf.bind()
    wf.bind_arrays()
    wf._create_workflow_run()

    app, DB = db_cfg["app"], db_cfg["DB"]
    with app.app_context():

        # Check narg1 and narg2 are represented in node_arg
        q = """
        SELECT * 
        FROM arg
        JOIN node_arg na ON na.arg_id = arg.id
        WHERE arg.name IN ('narg1', 'narg2')
        """
        res = DB.session.execute(q).fetchall()
        DB.session.commit()

        assert len(res) == 18  # 2 args per node * 9 nodes
        names = [r.name for r in res]
        assert set(names) == {"narg1", "narg2"}

        # Check task_arg in the task arg table
        task_q = """
        SELECT * 
        FROM arg
        JOIN task_arg ta ON ta.arg_id = arg.id
        WHERE arg.name IN ('task_arg')"""
        task_args = DB.session.execute(task_q).fetchall()
        DB.session.commit()

        assert len(task_args) == 9  # 9 unique tasks, 1 task_arg each
        task_arg_names = [r.name for r in task_args]
        assert set(task_arg_names) == {"task_arg"}


def test_empty_array(db_cfg, client_env, tool):
    """Check that an empty array raises the appropriate error."""

    tt = tool.get_task_template(
        template_name='empty',
        command_template="",
        node_args=[],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    array = tt.create_array()
    wf = tool.create_workflow()
    with pytest.raises(ValueError):
        wf.add_array(array)