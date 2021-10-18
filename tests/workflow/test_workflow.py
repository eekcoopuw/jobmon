from jobmon.exceptions import NodeDependencyNotExistError

from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus

import pytest


def test_add_tasks_dependencynotexist(db_cfg, client_env):

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    t2 = BashTask("echo 2", executor_class="SequentialExecutor")
    t3 = BashTask("echo 3", executor_class="SequentialExecutor")
    t3.add_upstream(t2)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf1", name="TestWF1")
        wf.add_tasks([t1, t2])
        wf.bind()
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf2", name="TestWF2")
        wf.add_tasks([t1, t3])
        wf.bind()
    assert "Upstream" in str(excinfo.value)
    wf = UnknownWorkflow("wf3", name="TestWF3", executor_class="SequentialExecutor")
    wf.add_tasks([t1, t2, t3])
    wf.run()
    assert len(wf.tasks) == 3
    wf = UnknownWorkflow("wf4", name="TestWF4", executor_class="SequentialExecutor")
    wf.add_tasks([t1])
    wf.add_tasks([t2])
    wf.add_tasks([t3])
    wf.bind()
    assert len(wf.tasks) == 3


def test_workflow_get_errors(db_cfg, client_env):
    """test that num attempts gets reset on a resume"""

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor
    from jobmon.serializers import SerializeExecutorTaskInstanceErrorLog

    # setup workflow 1
    tool = Tool()
    workflow1 = tool.create_workflow(name="test_workflow_get_errors")
    executor = SequentialExecutor()
    workflow1.set_executor(executor)
    task_a = BashTask("sleep 5", executor_class="SequentialExecutor",
                      max_attempts=3)
    workflow1.add_task(task_a)
    task_b = BashTask("sleep 6", executor_class="SequentialExecutor",
                      max_attempts=3)
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
        DB.session.execute("""
            UPDATE workflow_run
            SET status ='{s}'
            WHERE id={wfr_id}""".format(s=WorkflowRunStatus.RUNNING,
                                        wfr_id=wfr_1.workflow_run_id))
        DB.session.execute("""
            INSERT INTO task_instance (workflow_run_id, task_id, status)
            VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_a.task_id,
                s=TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR))
        ti = DB.session.execute(
            "SELECT max(id) from task_instance where task_id={}".format(task_a.task_id)
        ).fetchone()
        ti_id_a = ti[0]
        DB.session.execute("""
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(s=TaskStatus.RUNNING,
                                      t_id=task_a.task_id))
        DB.session.execute("""
            INSERT INTO task_instance (workflow_run_id, task_id, status)
            VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_b.task_id,
                s=TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR))
        ti = DB.session.execute(
            "SELECT max(id) from task_instance where task_id={}".format(task_b.task_id)
        ).fetchone()
        ti_id_b = ti[0]
        DB.session.execute("""
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(s=TaskStatus.RUNNING,
                                      t_id=task_b.task_id))
        DB.session.commit()

    # log task_instance fatal error for task_a
    app_route = f"/worker/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "bla bla bla"},
        request_type='post'
    )
    assert return_code == 200

    # log task_instance fatal error - 2nd error for task_a
    app_route = f"/worker/task_instance/{ti_id_a}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "ble ble ble"},
        request_type='post'
    )
    assert return_code == 200

    # log task_instance fatal error for task_b
    app_route = f"/worker/task_instance/{ti_id_b}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "cla cla cla"},
        request_type='post'
    )
    assert return_code == 200

    # make sure we see the 2 tasks in the workflow_errors(task_a and task_b)
    # and task_b one has 1 task_instance_error_log
    workflow_errors = workflow1.get_errors()
    assert type(workflow_errors) == dict
    assert len(workflow_errors) == 2
    task_b_errors = workflow_errors[task_b.task_id]
    assert task_b_errors['task_instance_id'] == ti_id_b
    error_log_b = task_b_errors['error_log']
    assert type(error_log_b) == list
    assert len(error_log_b) == 1
    err_1st_b = error_log_b[0]
    assert type(err_1st_b) == dict
    assert err_1st_b['description'] == "cla cla cla"

