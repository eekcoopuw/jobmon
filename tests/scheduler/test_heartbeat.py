from multiprocessing import Queue

import pytest

from jobmon.exceptions import ResumeSet


class MockSchedulerProc:

    def is_alive(self):
        return True


def test_heartbeat(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = UnknownWorkflow("my_beating_heart",
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=1)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)
    scheduler.heartbeat()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT workflow_run.heartbeat_date > UTC_TIMESTAMP()
        FROM workflow_run
        WHERE workflow_run.id = :workflow_run_id"""
        res = DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id}
                                 ).fetchone()
        DB.session.commit()
    assert res[0] == 0


def test_heartbeat_raises_error(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = UnknownWorkflow("my_heartbeat_error",
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=1)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    with pytest.raises(ResumeSet):
        scheduler.heartbeat()


def test_heartbeat_propagate_error(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = UnknownWorkflow("heartbeat_propagate_error",
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=1)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    q = Queue()
    scheduler.run_scheduler(status_queue=q)
    assert q.get() == "ALIVE"

    with pytest.raises(ResumeSet):
        q.get().re_raise()
