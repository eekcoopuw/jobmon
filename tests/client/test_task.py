import pytest
from sqlalchemy.sql import text

from jobmon.client.task import Task
from jobmon.client.tool import Tool


@pytest.fixture
def task_template(db_cfg, client_env):
    tool = Tool()
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


def test_good_names():
    """tests that a few legal names return as valid"""
    assert Task.is_valid_job_name("fred")
    assert Task.is_valid_job_name("fred123")
    assert Task.is_valid_job_name("fred_and-friends")


def test_bad_names():
    """tests that invalid names return a ValueError"""
    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("bad/dog")
    assert "special" in str(exc.value)


def test_equality(task_template):
    """tests that 2 identical tasks are equal and that non-identical tasks
    are not equal"""
    a = task_template.create_task(arg="a")
    a_again = task_template.create_task(arg="a")
    assert a == a_again

    b = task_template.create_task(arg="b", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_hash_name_compatibility(task_template):
    """test that name based on hash"""
    a = task_template.create_task(arg="a")
    assert "task_" + str(hash(a)) == a.name


def test_task_attribute(db_cfg, task_template):
    """Test that you can add task attributes to Bash and Python tasks"""
    from jobmon.client.workflow_run import WorkflowRun
    from jobmon.server.web.models.task_attribute import TaskAttribute
    from jobmon.server.web.models.task_attribute_type import TaskAttributeType

    tool = Tool()
    workflow1 = tool.create_workflow(name="test_task_attribute")
    task1 = task_template.create_task(
        arg="sleep 2", task_attributes={'LOCATION_ID': 1, 'AGE_GROUP_ID': 5, 'SEX': 1},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"}
    )
    task2 = task_template.create_task(
        arg="sleep 3", task_attributes=["NUM_CORES", "NUM_YEARS"],
        cluster_name="sequential",
        compute_resources={"queue": "null.q"}
    )

    task3 = task_template.create_task(
        arg="sleep 4", task_attributes={'NUM_CORES': 3, 'NUM_YEARS': 5},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"}
    )
    workflow1.add_tasks([task1, task2, task3])
    workflow1.bind()
    client_wfr = WorkflowRun(workflow1.workflow_id)
    client_wfr.bind(workflow1.tasks)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT task_attribute_type.name, task_attribute.value, task_attribute_type.id
        FROM task_attribute
        INNER JOIN task_attribute_type
            ON task_attribute.task_attribute_type_id = task_attribute_type.id
        WHERE task_attribute.task_id IN (:task_id_1, :task_id_2, :task_id_3)
        ORDER BY task_attribute_type.name, task_id
        """
        resp = DB.session.query(TaskAttribute.value, TaskAttributeType.name,
                                TaskAttributeType.id).\
            from_statement(text(query)).params(task_id_1=task1.task_id,
                                               task_id_2=task2.task_id,
                                               task_id_3=task3.task_id).all()
        values = [tup[0] for tup in resp]
        names = [tup[1] for tup in resp]
        ids = [tup[2] for tup in resp]
        expected_vals = ['5', '1', None, '3', None, '5', '1']
        expected_names = ['AGE_GROUP_ID', 'LOCATION_ID', 'NUM_CORES', 'NUM_CORES', 'NUM_YEARS',
                          'NUM_YEARS', 'SEX']

        assert values == expected_vals
        assert names == expected_names
        assert ids[2] == ids[3]  # will fail if adding non-unique task_attribute_types
        assert ids[4] == ids[5]


def test_executor_parameter_copy(tool, task_template):
    """test that 1 executorparameters object passed to multiple tasks are distinct objects,
    and scaling 1 task does not scale the others"""

    # Use SGEExecutor for adjust methods, but the executor is never called
    # Therefore, not an SGEIntegration test
    compute_resources = {
        "m_mem_free": '1G', "max_runtime_seconds": 60,
        "num_cores": 1, "queue": 'all.q'
    }

    task1 = task_template.create_task(
        name='foo', arg="echo foo", compute_resources=compute_resources
    )
    task2 = task_template.create_task(
        name='bar', arg="echo bar", compute_resources=compute_resources
    )

    # Ensure memory addresses are different
    assert id(task1.compute_resources) != id(task2.compute_resources)
