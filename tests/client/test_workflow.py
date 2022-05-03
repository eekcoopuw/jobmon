import logging

import pytest

from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (
    WorkflowAlreadyComplete,
    DuplicateNodeArgsError,
    WorkflowAlreadyExists,
    NodeDependencyNotExistError,
    WorkflowNotResumable,
)


def test_wfargs_update(tool):
    """test that 2 workflows with different names, have different ids and tasks"""
    from jobmon.client.workflow_run import WorkflowRun

    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )
    t3 = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 3", upstream_tasks=[t2]
    )

    t4 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t5 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t4]
    )
    t6 = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 3", upstream_tasks=[t5]
    )

    wfa1 = "v1"
    wf1 = tool.create_workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.bind()

    wfa2 = "v2"
    wf2 = tool.create_workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.bind()

    # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
    assert wf1.workflow_id != wf2.workflow_id

    # Make sure the second Workflow has a distinct hash
    assert hash(wf1) != hash(wf2)

    # Make sure the second Workflow has a distinct set of Tasks
    wfr1 = WorkflowRun(wf1)
    wfr1.bind()
    wfr2 = WorkflowRun(wf2)
    wfr2.bind()
    assert not (
        set([t.task_id for _, t in wf1.tasks.items()])
        & set([t.task_id for _, t in wf2.tasks.items()])
    )


def test_attempt_resume_on_complete_workflow(tool):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    from jobmon.client.workflow_run import WorkflowRun

    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    wf1 = tool.create_workflow(name="attempt_resume_on_completed")
    wf1.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    wf1.bind()
    wfr1 = WorkflowRun(wf1)
    wfr1.bind()
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)
    wfr1._update_status(WorkflowRunStatus.DONE)

    # second workflow shouldn't be able to start
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    workflow2 = tool.create_workflow(
        wf1.workflow_args, name="attempt_resume_on_completed"
    )
    workflow2.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    workflow2.bind()
    with pytest.raises(WorkflowAlreadyComplete):
        workflow2._create_workflow_run()


def test_resume_with_old_and_new_workflow_attributes(db_cfg, tool):
    """Should allow a resume, and should not fail on duplicate workflow_attribute keys"""
    from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
    from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType

    # Create identical dags
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    wf1 = tool.create_workflow(
        name="attempt_resume_on_failed",
        workflow_attributes={"location_id": 5, "year": "2019"},
    )
    wf1.add_tasks([t1, t2])

    # bind workflow to db and move to ERROR state
    wf1.bind()
    wfr1 = wf1._create_workflow_run()
    wfr1._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)
    wfr1._update_status(WorkflowRunStatus.ERROR)

    # second workflow
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    workflow2 = tool.create_workflow(
        wf1.workflow_args,
        name="attempt_resume_on_failed",
        workflow_attributes={"location_id": 5, "year": "2022", "sex": "F"},
    )
    workflow2.add_tasks([t1, t2])

    # bind workflow to db and run resume
    workflow2.bind()
    workflow2._create_workflow_run(resume=True)

    # check database entries are populated correctly
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_attributes = (
            DB.session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == set(
        [("location_id", "5"), ("year", "2022"), ("sex", "F")]
    )


def test_multiple_active_race_condition(tool, task_template):
    """test that we cannot create 2 workflow runs simultaneously"""

    # create initial workflow
    t1 = task_template.create_task(arg="sleep 1")
    workflow1 = tool.create_workflow(name="created_race_condition")
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._create_workflow_run()

    # create identical workflow
    t2 = task_template.create_task(arg="sleep 1")
    workflow2 = tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args
    )
    workflow2.add_tasks([t2])
    workflow2.bind()
    with pytest.raises(WorkflowNotResumable):
        workflow2._create_workflow_run(resume=True, resume_timeout=1)


def test_workflow_identical_args(tool, task_template):
    """test that 2 workflows with identical arguments can't exist
    simultaneously"""

    # first workflow bound
    wf1 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 1")
    wf1.add_task(task)
    wf1.bind()
    wf1._create_workflow_run()

    # tries to create an identical workflow without the restart flag
    wf2 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 1")
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.bind()
        wf2._create_workflow_run()


def test_add_same_node_args_twice(client_env):
    from jobmon.client.tool import Tool

    tool = Tool()
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
        op_args=[],
    )
    a = tt.create_task(node_arg="a", task_arg="a")
    b = tt.create_task(node_arg="a", task_arg="b")

    workflow = tool.create_workflow()
    workflow.add_task(a)
    with pytest.raises(DuplicateNodeArgsError):
        workflow.add_task(b)


def test_numpy_array_node_args(tool):
    """Test passing an object (set) that is not JSON serializable to node and task args."""
    workflow = tool.create_workflow(name="numpy_test_wf")
    template = tool.get_task_template(
        template_name="numpy_test_template",
        command_template="echo {node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
    )
    task = template.create_task(node_arg={1, 2}, task_arg={3, 4})
    workflow.add_tasks([task])
    workflow.bind()
    assert workflow.workflow_id


def test_empty_workflow(tool):
    """
    Create a real_dag with no Tasks. Call all the creation methods and check
    that it raises no Exceptions.
    """

    workflow = tool.create_workflow(name="test_empty_real_dag")

    with pytest.raises(RuntimeError):
        workflow.run()


@pytest.mark.skip()
def test_compute_resources(db_cfg, client_env):
    """Test user passed cluster_resources. Need to test: 1. task with compute resources,
    no workflow resources 2. task with no compute resources, workflow resources 3. tasks with
    less resources than workflow"""
    from jobmon.client.tool import Tool

    tool = Tool(name="cluster_resource_test")
    wf_compute_resources = {
        "sequential": {
            "num_cores": 2,
            "mem": "2G",
            "max_runtime_seconds": 10,
            "queue": "null.q",
            "resource_scales": {"runtime": 0.7},
        },
        "buster": {"mem": "5G"},
    }
    workflow_1 = tool.create_workflow(
        name="compute_resource_1", compute_resources=wf_compute_resources
    )
    template = tool.get_task_template(
        template_name="my_template",
        command_template="echo {node_arg}",
        node_args=["node_arg"],
    )
    task_compute_resource = {
        "sequential": {
            "num_cores": 1,
            "mem": "1G",
            "max_runtime_seconds": 1,
            "queue": "null.q",
            "resource_scales": {"runtime": 0.5},
        },
        "buster": {"mem": "5G"},
    }
    # Use case: Task compute resources, no workflow resources
    task_1 = template.create_task(
        name="task_1",
        node_arg={1},
        compute_resources=task_compute_resource,
        cluster_name="sequential",
    )

    # Use case: No Task compute resources, inherit from workflow compute resources
    task_2 = template.create_task(
        name="task_2", node_arg={2}, cluster_name="sequential"
    )

    # TODO: Add test case when we implement partial compute resource dicts
    # Use case: No Task compute resources, inherit from workflow compute resources
    # Use case: Minimal task resources, keep task runtime, inherit other resources from wf
    # task_3 = template.create_task(name="task_3", node_arg={3},
    #                               compute_resources={"max_runtime_seconds": 8},
    #                               cluster_name="sequential")

    workflow_1.add_tasks([task_1, task_2])
    workflow_1.bind()

    client_wfr = ClientWorkflowRun(
        workflow_id=workflow_1.workflow_id, executor_class="Sequential"
    )
    client_wfr.bind(workflow_1.tasks, False, workflow_1._chunk_size)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = "SELECT requested_resources " "FROM task_resources "
        res = DB.session.execute(query).fetchall()
        DB.session.commit()
    assert res[0][0] == f"{task_compute_resource['sequential']}"
    assert res[1][0] == f"{wf_compute_resources['sequential']}"


def test_workflow_attribute(db_cfg, tool, client_env, task_template):
    """Test the workflow attributes feature"""
    from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
    from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType

    wf1 = tool.create_workflow(
        name="test_wf_attributes",
        workflow_attributes={"location_id": 5, "year": 2019, "sex": 1},
    )

    t1 = task_template.create_task(arg="exit -0")
    wf1.add_task(t1)
    wf1.bind()

    # check database entries are populated correctly
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_attributes = (
            DB.session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == set(
        [("location_id", "5"), ("year", "2019"), ("sex", "1")]
    )

    # Add and update attributes
    wf1.add_attributes({"age_group_id": 1, "sex": 2})

    with app.app_context():
        wf_attributes = (
            DB.session.query(WorkflowAttributeType.name, WorkflowAttribute.value)
            .join(
                WorkflowAttribute,
                WorkflowAttribute.workflow_attribute_type_id
                == WorkflowAttributeType.id,
            )
            .filter(WorkflowAttribute.workflow_id == wf1.workflow_id)
            .all()
        )
    assert set(wf_attributes) == set(
        [("location_id", "5"), ("year", "2019"), ("sex", "2"), ("age_group_id", "1")]
    )

    # Test workflow w/o attributes
    wf2 = tool.create_workflow(
        name="test_empty_wf_attributes",
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    wf2.add_task(t1)
    wf2.bind()

    with app.app_context():
        wf_attributes = (
            DB.session.query(WorkflowAttribute)
            .filter_by(workflow_id=wf2.workflow_id)
            .all()
        )

    assert wf_attributes == []


def test_chunk_size(db_cfg, tool, client_env, task_template):

    wf_a = tool.create_workflow(name="test_wf_chunks_a", chunk_size=3)

    task_a = task_template.create_task(arg="echo a", upstream_tasks=[])  # To be clear
    wf_a.add_task(task_a)
    wf_a.bind()

    wf_b = tool.create_workflow(name="test_wf_chunks_b", chunk_size=10)
    task_b = task_template.create_task(arg="echo b", upstream_tasks=[])  # To be clear
    wf_b.add_task(task_b)
    wf_b.bind()

    assert wf_a._chunk_size == 3
    assert wf_b._chunk_size == 10


def test_add_tasks_dependencynotexist(db_cfg, tool, client_env, task_template):

    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")
    t3.add_upstream(t2)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF1")
        wf.add_tasks([t1, t2])
        wf.bind()
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF2")
        wf.add_tasks([t1, t3])
        wf.bind()
    assert "Upstream" in str(excinfo.value)
    wf = tool.create_workflow(name="TestWF3")
    wf.add_tasks([t1, t2, t3])
    wf.bind()
    assert len(wf.tasks) == 3
    wf = tool.create_workflow(name="TestWF4")
    wf.add_tasks([t1])
    wf.add_tasks([t2])
    wf.add_tasks([t3])
    wf.bind()
    assert len(wf.tasks) == 3


def test_workflow_validation(tool, task_template, caplog):
    """Test the workflow.validate() function, and ensure idempotency"""
    too_many_cores = {"cores": 1000, "queue": "null.q", "runtime": "01:02:33"}
    good_resources = {"cores": 20, "queue": "null.q", "runtime": "01:02:33"}
    t1 = task_template.create_task(
        arg="echo 1", compute_resources=too_many_cores, cluster_name="multiprocess"
    )
    wf1 = tool.create_workflow()
    wf1.add_task(t1)

    with pytest.raises(ValueError):
        wf1.validate()  # Max cores on multiprocess null.q is 20. Should fail

    # Without fail set, validate and check coercion
    caplog.clear()
    with caplog.at_level(logging.INFO):
        wf1.validate(fail=False)
        assert (
            "failed validation, reasons: ResourceError: provided cores 1000 exceeds queue"
            in caplog.records[-1].message
        )

    # Try again for idempotency
    caplog.clear()
    with caplog.at_level(logging.INFO):
        wf1.validate(fail=False)
        assert (
            "failed validation, reasons: ResourceError: provided cores 1000 exceeds queue"
            in caplog.records[-1].message
        )

    # Try with valid resources
    t2 = task_template.create_task(
        arg="echo 1", compute_resources=good_resources, cluster_name="multiprocess"
    )
    wf2 = tool.create_workflow()
    wf2.add_task(t2)
    wf2.validate()


def test_workflow_get_errors(tool, task_template, db_cfg):
    """test that num attempts gets reset on a resume."""

    from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
    from jobmon.server.web.models.task_status import TaskStatus
    from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus

    # setup workflow 1
    workflow1 = tool.create_workflow(name="test_workflow_get_errors")
    task_a = task_template.create_task(arg="sleep 5", max_attempts=1)
    workflow1.add_task(task_a)
    task_b = task_template.create_task(arg="sleep 6", max_attempts=1)
    workflow1.add_task(task_b)

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
            VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_a.task_id,
                s=TaskInstanceStatus.LAUNCHED,
            )
        )
        ti = DB.session.execute(
            "SELECT id from task_instance where task_id={}".format(task_a.task_id)
        ).fetchone()
        ti_id_a = ti[0]
        DB.session.execute(
            """
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(
                s=TaskStatus.INSTANTIATING, t_id=task_a.task_id
            )
        )
        DB.session.execute(
            """
            INSERT INTO task_instance (workflow_run_id, task_id, status)
            VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_b.task_id,
                s=TaskInstanceStatus.LAUNCHED,
            )
        )
        ti = DB.session.execute(
            "SELECT id from task_instance where task_id={}".format(task_b.task_id)
        ).fetchone()
        ti_id_b = ti[0]
        DB.session.execute(
            """
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(
                s=TaskStatus.INSTANTIATING, t_id=task_b.task_id
            )
        )
        DB.session.commit()

    # log task_instance fatal error for task_a
    app_route = f"/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={
            "error_state": TaskInstanceStatus.ERROR,
            "error_message": "bla bla bla",
        },
        request_type="post",
    )
    assert return_code == 200

    # log task_instance fatal error - 2nd error for task_a
    app_route = f"/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "ble ble ble"},
        request_type="post",
    )
    assert return_code == 200

    # log task_instance fatal error for task_b
    app_route = f"/task_instance/{ti_id_b}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "cla cla cla"},
        request_type="post",
    )
    assert return_code == 200

    # make sure we see the 2 tasks in the workflow_errors(task_a and task_b)
    # and task_b one has 1 task_instance_error_log
    workflow_errors = workflow1.get_errors()
    assert type(workflow_errors) == dict
    assert len(workflow_errors) == 2
    task_b_errors = workflow_errors[task_b.task_id]
    assert task_b_errors["task_instance_id"] == ti_id_b
    error_log_b = task_b_errors["error_log"]
    assert type(error_log_b) == list
    assert len(error_log_b) == 1
    err_1st_b = error_log_b[0]
    assert type(err_1st_b) == dict
    assert err_1st_b["description"] == "cla cla cla"
