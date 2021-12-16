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

@pytest.fixture
def task_template_dummy(tool):
    tt = tool.get_task_template(
        template_name="dummy_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


def test_create_array(db_cfg, client_env, task_template):
    array = task_template.create_array(arg="echo 1")
    assert array.compute_resources == task_template.default_compute_resources_set["sequential"]


def test_array_bind(db_cfg, client_env, task_template_dummy, tool):
    task_template = task_template_dummy

    array = task_template.create_array(arg="echo 10",
                                       compute_resources={"queue": "null.q"})
    wf = tool.create_workflow()

    wf.add_array(array)
    wf.bind()
    wf._create_workflow_run()

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

    # Assert the bound task has the correct array ID
    with app.app_context():
        task_query = """
        SELECT array_id
        FROM task
        WHERE id = {}
        """.format(
            list(array.tasks.values())[0].task_id
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
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )

    array = rich_task_template.create_array(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        narg2=["a", "b", "c"],
        op_arg="baz",
        compute_resources={"queue": "null.q"}
    )

    assert len(array.tasks) == 9  # Created on init
    wf = tool.create_workflow()
    wf.add_array(array)

    assert len(wf.tasks) == 9  # Tasks bound to workflow

    # Assert tasks templated correctly
    commands = [t.command for t in array.tasks.values()]
    assert "foo bar 1 c baz" in commands
    assert "foo bar 3 a baz" in commands

    # Check node and task args are recorded in the proper tables
    wf.bind()
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

    # Define individual node_args, and expect to get a list of one task
    three_c_node_args = {"narg1": 3, "narg2": "c"}
    three_c_tasks = array.get_tasks_by_node_args(**three_c_node_args)
    assert len(three_c_tasks) == 1

    # Define out of scope node_args, and expect to get a empty list
    x_node_args = {"narg1": 3, "narg2": "x"}
    x_tasks = array.get_tasks_by_node_args(**x_node_args)
    assert len(x_tasks) == 0

    # Define narg1 only, expect to see a list of 3 tasks
    three_node_args = {"narg1": 3}
    three_tasks = array.get_tasks_by_node_args(**three_node_args)
    assert len(three_tasks) == 3

    # Define an empty dict, expect to see a list of 9 tasks
    empty_node_args = {}
    all_tasks = array.get_tasks_by_node_args(**empty_node_args)
    assert len(all_tasks) == 9

    # Define a list(3,2 items)-valued case for narg1 and narg2, expect to see 6
    empty_node_args = {"narg1": [1, 2, 3], "narg2": ["a", "b"]}
    all_tasks = array.get_tasks_by_node_args(**empty_node_args)
    assert len(all_tasks) == 6

    # Define a task_template_name in-scope valid node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    two_wf_tasks = wf.get_tasks_by_node_args(
        task_template_name="simple_template", **two_node_args
    )
    assert len(two_wf_tasks) == 3

    # Define a task_template_name out-of-scope node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    with pytest.raises(ValueError):
        two_wf_tasks = wf.get_tasks_by_node_args(
            task_template_name="OUT_OF_SCOPE_simple_template", **two_node_args
        )


def test_empty_array(db_cfg, client_env, tool):
    """Check that an empty array raises the appropriate error."""

    tt = tool.get_task_template(
        template_name="empty",
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


def test_record_array_batch_num(db_cfg, client_env, tool):
    from jobmon.client.distributor.distributor_array import DistributorArray
    from jobmon.client.distributor.distributor_task import DistributorTask
    from jobmon.requester import Requester

    tt = tool.get_task_template(
        template_name="whatever",
        command_template="{command} {task_arg} {narg1}",
        node_args=["narg1"],
        task_args=["task_arg"],
        op_args=["command"],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )

    array = tt.create_array(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        compute_resources={"queue": "null.q"}
    )

    assert len(array.tasks) == 3  # Created on init
    wf = tool.create_workflow()
    wf.add_array(array)
    wf.bind()
    #wf.bind_arrays()
    wfr = wf._create_workflow_run()
    requester = Requester(client_env)
    distributor_array = DistributorArray(array_id=array.array_id,
                                         task_resources_id=array.task_resources.id,
                                         requested_resources=array.compute_resources,
                                         name="example_array",
                                         requester=requester
                                         )
    dts = [
        DistributorTask(task_id=t.task_id,
                        array_id=array.array_id,
                        name='array_ti',
                        command=t.command,
                        requested_resources=t.compute_resources,
                        requester=requester)
        for t in array.tasks.values()
    ]
    # Move all tasks to Q state
    for tid in (t.task_id for t in array.tasks.values()):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )
    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    _, _ = requester.send_request(
        app_route="/task_instance/record_array_batch_num/1",
        message={'task_instance_ids': [dtis_1.task_instance_id, dtis_2.task_instance_id, dtis_3.task_instance_id]},
        request_type='post'
    )
    sorted_tiids = [dtis_1.task_instance_id, dtis_2.task_instance_id, dtis_3.task_instance_id]
    sorted_tiids.sort()
    app, DB = db_cfg["app"], db_cfg["DB"]
    with app.app_context():
        for i in range(len(sorted_tiids)):
            sql = f"""SELECT array_step_id
                FROM task_instance
                WHERE id = {sorted_tiids[i]}"""
            step_id = DB.session.execute(sql).fetchone()["array_step_id"]
            assert step_id == i + 1