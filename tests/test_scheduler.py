import time

import pytest

from jobmon.client_config import client_config
from jobmon.client.workflow.bash_task import BashTask
from jobmon.client.workflow.workflow import Workflow

from jobmon.execution.scheduler import JobInstanceScheduler
from jobmon.execution.strategies.sequential import SequentialExecutor
from jobmon.execution.strategies.dummy import DummyExecutor


@pytest.fixture(scope='function')
def dummy_scheduler():
    # We don't want this queueing jobs in conflict with the SGE daemons...
    # but we do need it to subscribe to status updates for reconciliation
    # tests. Start this thread manually.
    old_heartbeat_interval = client_config.heartbeat_interval
    client_config.heartbeat_interval = 5
    executor = DummyExecutor()
    scheduler = JobInstanceScheduler(executor=executor)
    yield scheduler
    client_config.heartbeat_interval = old_heartbeat_interval


def test_instantiate_queued_jobs(real_jsm_jqs, db_cfg):
    executor = SequentialExecutor()
    scheduler = JobInstanceScheduler(executor=executor)

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = Workflow("my_simple_dag", executor_class="SequentialExecutor",
                        seconds_until_timeout=1)
    workflow.add_tasks([t1])

    with pytest.raises(RuntimeError):
        workflow.run()

    scheduler.instantiate_queued_jobs()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT status
        FROM job_instance
        WHERE job_id = 1"""
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    assert res[0] == "D"


def test_reconcile(real_jsm_jqs, db_cfg):
    executor = DummyExecutor()
    scheduler = JobInstanceScheduler(executor=executor)

    t1 = BashTask("echo 1", executor_class="DummyExecutor")
    workflow = Workflow("my_simple_dag", executor_class="DummyExecutor",
                        seconds_until_timeout=1)
    workflow.add_tasks([t1])

    with pytest.raises(RuntimeError):
        workflow.run()

    scheduler.instantiate_queued_jobs()
    # Since we are using the 'dummy' executor, we never actually do
    # any work. It should stay in "B" state
    state = ''
    maxretries = 10
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = "SELECT status FROM job_instance"
    i = 0
    while i < maxretries:
        i += 1
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        if res[0] == "B":
            state = "B"
            break
        time.sleep(5)
    if state != "B":
        raise Exception("The init status failed to be set to B")

    # job will move into lost track because it never logs a heartbeat
    i = 0
    while i < maxretries:
        scheduler.reconcile()
        i += 1
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        if res[0] != "B":
            break
        time.sleep(5)
    assert res[0] == "U"
