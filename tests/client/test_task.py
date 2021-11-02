import pytest
from sqlalchemy.sql import text

from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.workflow_run import WorkflowRun
from jobmon.constants import WorkflowRunStatus, TaskStatus, TaskInstanceStatus
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.serializers import SerializeTaskInstanceErrorLog


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


def test_default_task_name(task_template):
    """test that name based on hash"""
    # noral case
    a = task_template.create_task(arg="a")
    assert a.name == "simple_template_1-a"
    # long name
    a = task_template.create_task(arg="a" * 256)
    assert a.name == ("simple_template_1-" + "a" * 256)[0:249]
    # special char
    a = task_template.create_task(arg="abc'abc/abc")
    assert a.name == "simple_template_1-abc_abc_abc"


def test_task_attribute(db_cfg, tool):
    """Test that you can add task attributes to Bash and Python tasks"""

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
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
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
                s=TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR,
            )
        )
        ti = DB.session.execute(
            "SELECT max(id) from task_instance where task_id={}".format(task_a.task_id)
        ).fetchone()
        ti_id = ti[0]
        DB.session.execute(
            """
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(
                s=TaskStatus.RUNNING, t_id=task_a.task_id
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

    # log task_instance fatal error - 2nd error
    app_route = f"/task_instance/{ti_id}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "ble ble ble"},
        request_type="post",
    )
    assert return_code == 200

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        t = DB.session.query(Task).filter_by(id=task_a.task_id).one()
        assert t.status == TaskStatus.ERROR_FATAL
        DB.session.commit()

    # make sure that the 2 errors logged above are counted for in the request_type='get'
    rc, response = workflow1.requester.send_request(
        app_route=f"/task_instance/{ti_id}/task_instance_error_log",
        message={},
        request_type="get",
    )
    all_errors = [
        SerializeTaskInstanceErrorLog.kwargs_from_wire(j)
        for j in response["task_instance_error_log"]
    ]
    assert len(all_errors) == 2

    # make sure we see the 2 task_instance_error_log when checking
    # on the existing task_a, which should return a dict
    # produced in task.py
    task_errors = task_a.get_errors()
    assert type(task_errors) == dict
    assert len(task_errors) == 2
    assert task_errors["task_instance_id"] == ti_id
    error_log = task_errors["error_log"]
    assert type(error_log) == list
    err_1st = error_log[0]
    err_2nd = error_log[1]
    assert type(err_1st) == dict
    assert type(err_2nd) == dict
    assert err_1st["description"] == "bla bla bla"
    assert err_2nd["description"] == "ble ble ble"


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
        assert t.status == TaskStatus.REGISTERED
        DB.session.commit()


def test_resource_usage(db_cfg, client_env):
    """Test Task resource usage method."""
    from jobmon.client.tool import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    workflow = tool.create_workflow(name="resource_usage_test_wf")
    template = tool.get_task_template(
        template_name="resource_usage_test_template",
        command_template="echo a",
    )
    task = template.create_task()
    workflow.add_tasks([task])
    workflow.run()

    # Add fake resource usage to the TaskInstance
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE task_instance
        SET nodename = 'SequentialNode', wallclock = 12, maxpss = 1234
        WHERE task_id = :task_id"""
        DB.session.execute(sql, {"task_id": task.task_id})
        DB.session.commit()
    used_task_resources = task.resource_usage()
    assert used_task_resources == {
        "memory": "1234",
        "nodename": "SequentialNode",
        "num_attempts": 1,
        "runtime": "12",
    }
