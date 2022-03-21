import pytest
from unittest.mock import patch, PropertyMock
from sqlalchemy.sql import text

from jobmon.constants import WorkflowRunStatus, TaskStatus, TaskInstanceStatus
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType


def test_good_names():
    """tests that a few legal names return as valid"""
    from jobmon.client.task import Task

    assert Task.is_valid_job_name("fred")
    assert Task.is_valid_job_name("fred123")
    assert Task.is_valid_job_name("fred_and-friends")


def test_bad_names():
    """tests that invalid names return a ValueError"""
    from jobmon.client.task import Task

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


def test_default_task_name(task_template):
    """test that name based on hash"""
    # noral case
    a = task_template.create_task(arg="a")
    assert a.name == "simple_template_arg-a"
    # long name
    a = task_template.create_task(arg="a" * 256)
    assert a.name == ("simple_template_arg-" + "a" * 256)[0:249]
    # special char
    a = task_template.create_task(arg="abc'abc/abc")
    assert a.name == "simple_template_arg-abc_abc_abc"
    # spaces
    a = task_template.create_task(arg="echo 10")
    assert a.name == "simple_template_arg-echo_10"


def test_task_attribute(db_cfg, tool):
    """Test that you can add task attributes to Bash and Python tasks"""
    from jobmon.client.workflow_run import WorkflowRun

    workflow1 = tool.create_workflow(name="test_task_attribute")
    task_template = tool.active_task_templates["simple_template"]
    task1 = task_template.create_task(
        arg="sleep 2",
        task_attributes={"LOCATION_ID": 1, "AGE_GROUP_ID": 5, "SEX": 1},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )
    task2 = task_template.create_task(
        arg="sleep 3",
        task_attributes=["NUM_CORES", "NUM_YEARS"],
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )

    task3 = task_template.create_task(
        arg="sleep 4",
        task_attributes={"NUM_CORES": 3, "NUM_YEARS": 5},
        cluster_name="sequential",
        compute_resources={"queue": "null.q"},
    )
    workflow1.add_tasks([task1, task2, task3])
    workflow1.bind()
    client_wfr = WorkflowRun(workflow1)
    client_wfr.bind()

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
        resp = (
            DB.session.query(
                TaskAttribute.value, TaskAttributeType.name, TaskAttributeType.id
            )
            .from_statement(text(query))
            .params(
                task_id_1=task1.task_id,
                task_id_2=task2.task_id,
                task_id_3=task3.task_id,
            )
            .all()
        )
        values = [tup[0] for tup in resp]
        names = [tup[1] for tup in resp]
        ids = [tup[2] for tup in resp]
        expected_vals = ["5", "1", None, "3", None, "5", "1"]
        expected_names = [
            "AGE_GROUP_ID",
            "LOCATION_ID",
            "NUM_CORES",
            "NUM_CORES",
            "NUM_YEARS",
            "NUM_YEARS",
            "SEX",
        ]

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
        "m_mem_free": "1G",
        "max_runtime_seconds": 60,
        "num_cores": 1,
        "queue": "all.q",
    }

    task1 = task_template.create_task(
        name="foo", arg="echo foo", compute_resources=compute_resources
    )
    task2 = task_template.create_task(
        name="bar", arg="echo bar", compute_resources=compute_resources
    )

    # Ensure memory addresses are different
    assert id(task1.compute_resources) != id(task2.compute_resources)


def test_get_errors(db_cfg, tool):
    """test that num attempts gets reset on a resume"""
    from jobmon.server.web.models.task import Task

    # setup workflow 1
    workflow1 = tool.create_workflow(name="test_task_instance_error_fatal")
    task_a = tool.active_task_templates["simple_template"].create_task(
        arg="sleep 5", max_attempts=1
    )
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    wfr_1 = workflow1._create_workflow_run()

    # for an just initialized task, get_errors() should be None
    assert task_a.get_errors() is None

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # fake workflow run
        DB.session.execute(
            """
            UPDATE workflow_run
            SET status ='{s}'
            WHERE id={wfr_id}""".format(
                s=WorkflowRunStatus.RUNNING, wfr_id=wfr_1.workflow_run_id
            )
        )
        DB.session.execute(
            """
            INSERT INTO task_instance (workflow_run_id, task_id, status)
            VALUES ({wfr_id}, {t_id}, '{s}')
            """.format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_a.task_id,
                s=TaskInstanceStatus.LAUNCHED,
            )
        )
        ti = DB.session.execute(
            "SELECT id from task_instance where task_id={}".format(task_a.task_id)
        ).fetchone()
        ti_id = ti[0]
        DB.session.execute(
            """
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(
                s=TaskStatus.INSTANTIATING, t_id=task_a.task_id
            )
        )
        DB.session.commit()

    # log task_instance fatal error
    app_route = f"/task_instance/{ti_id}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "bla bla bla"},
        request_type="post",
    )
    assert return_code == 200

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        t = DB.session.query(Task).filter_by(id=task_a.task_id).one()
        assert t.status == TaskStatus.ERROR_FATAL
        DB.session.commit()

    # make sure we see the 2 task_instance_error_log when checking
    # on the existing task_a, which should return a dict
    # produced in task.py
    task_errors = task_a.get_errors()
    assert type(task_errors) == dict
    assert task_errors["task_instance_id"] == ti_id
    error_log = task_errors["error_log"]
    assert type(error_log) == list
    err_1st = error_log[0]
    assert type(err_1st) == dict
    assert err_1st["description"] == "bla bla bla"


def test_reset_attempts_on_resume(db_cfg, tool):
    """test that num attempts gets reset on a resume"""
    from jobmon.server.web.models.task import Task

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts

    # setup workflow 1
    workflow1 = tool.create_workflow(name="test_reset_attempts_on_resume")
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    wfr_1 = workflow1._create_workflow_run()
    wfr_1._update_status(WorkflowRunStatus.ERROR)

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute(
            """
            UPDATE task
            SET status='{s}', num_attempts=3, max_attempts=3
            WHERE task.id={task_id}""".format(
                s=TaskStatus.ERROR_FATAL, task_id=task_a.task_id
            )
        )
        DB.session.commit()

    # create a second workflow and actually run it
    workflow2 = tool.create_workflow(
        name="test_reset_attempts_on_resume", workflow_args=workflow1.workflow_args
    )
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
    workflow2.add_task(task_a)
    workflow2.bind()
    workflow2._create_workflow_run(resume=True)

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        t = DB.session.query(Task).filter_by(id=task_a.task_id).one()
        assert t.max_attempts == 3
        assert t.num_attempts == 0
        assert t.status == TaskStatus.REGISTERING
        DB.session.commit()


def test_binding_length(db_cfg, client_env, tool):
    """Test that mysql exceptions return the appropriate error code."""

    from jobmon.exceptions import InvalidResponse

    # Test that args/attributes that are too long return sensible errors
    tt = tool.get_task_template(
        template_name="test_tt",
        command_template="{narg} {targ}",
        node_args=["narg"],
        task_args=["targ"],
    )
    # Task 1: too long task args (3 * 350 = 1050, max length=1000)
    task1 = tt.create_task(name="foo", narg="abc", targ="def" * 350)
    wf = tool.create_workflow()
    wf.add_task(task1)
    wf.bind()
    with pytest.raises(InvalidResponse) as resp:
        wfr1 = wf._create_workflow_run()
    exc_msg = resp.value.args[0]
    assert "Task Args are constrained to 1000 characters" in exc_msg
    assert "Unexpected status code 400" in exc_msg

    # task2: super long attributes
    task2 = tt.create_task(
        name="foo", narg="abc", targ="def", task_attributes={"hello": "world" * 60}
    )
    wf2 = tool.create_workflow()
    wf2.add_task(task2)
    wf2.bind()
    with pytest.raises(InvalidResponse) as resp2:
        wfr2 = wf2._create_workflow_run()
    exc_msg = resp2.value.args[0]
    assert "Task attributes are constrained to 255 characters" in exc_msg
    assert "Unexpected status code 400" in exc_msg
