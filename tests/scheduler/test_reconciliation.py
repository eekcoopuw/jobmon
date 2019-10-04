import time
import pytest

from jobmon.client import Workflow, BashTask


def test_unknown_state(db_cfg, client_env, dummy_scheduler):
    """Creates a job instance, gets an executor id so it can be in submitted
    to the batch executor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""

    # Queue a job
    task = BashTask(command="ls", name="dummyfbb", max_attempts=1)
    workflow = Workflow("foo", seconds_until_timeout=1,
                        executor_class="DummyExecutor")
    workflow.add_task(task)

    # add workflow info to db and then time out. Alternative would be to call
    # 5 or 6 underlying methods
    with pytest.raises(RuntimeError):
        workflow.run()

    # How long we wait for a JI to report it is running before reconciler moves
    # it to error state.
    dummy_scheduler.instantiate_queued_jobs()

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job gets moved to lost_track during reconciliation
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = """
        SELECT job_instance.status
        FROM job_instance
        JOIN job using (job_id)
        WHERE job_hash = :job_hash"""
    res = DB.session.execute(sql, {"job_hash": str(task.hash)}).fetchone()
    DB.session.commit()
    assert res[0] == "B"

    # sleep through the report by date
    time.sleep(dummy_scheduler.config.heartbeat_interval *
               dummy_scheduler.config.report_by_buffer)

    # job will move into lost track because it never logs a heartbeat
    dummy_scheduler.reconcile()
    res = DB.session.execute(sql, {"job_hash": str(task.hash)}).fetchone()
    DB.session.commit()
    assert res[0] == "U"

    # because we only allow 1 attempt job will move to E after job instance
    # moves to U
    job_list_manager = workflow.task_dag.job_list_manager
    job_list_manager._sync()
    assert len(job_list_manager.all_error) > 0


@pytest.fixture(scope='function')
def sequential_scheduler_no_execute(real_jsm_jqs, monkeypatch):
    # modify env
    from jobmon import config
    from jobmon.execution.strategies import sequential
    config.jobmon_server_sqdn = real_jsm_jqs["JOBMON_HOST"]
    config.jobmon_service_port = real_jsm_jqs["JOBMON_PORT"]

    # patch unwrap from sequential so the command doesn't execute
    scheduler = pytest.helpers.sequential_scheduler_instance(
        real_jsm_jqs["JOBMON_HOST"], real_jsm_jqs["JOBMON_PORT"], 4, 2, 2.1)

    def mock_unwrap(*args, **kwargs):
        pass
    monkeypatch.setattr(sequential, "unwrap", mock_unwrap)

    yield scheduler
    scheduler.stop()
    config.jobmon_server_sqdn = None
    config.jobmon_service_port = None


def test_log_executor_report_by(db_cfg, client_env,
                                sequential_scheduler_no_execute):
    task = BashTask(command="sleep 5", name="heartbeat_sleeper", num_cores=1,
                    max_runtime_seconds=500)
    workflow = Workflow("foo", seconds_until_timeout=1,
                        executor_class="SequentialExecutor")
    workflow.add_task(task)

    # add workflow info to db and then time out. Alternative would be to call
    # 5 or 6 underlying methods
    with pytest.raises(RuntimeError):
        workflow.run()

    # instantiate the job and then log a report by
    sequential_scheduler_no_execute.instantiate_queued_jobs()
    sequential_scheduler_no_execute._log_executor_report_by()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        q = """
        SELECT job_instance.submitted_date, job_instance.report_by_date
        FROM job_instance
        JOIN job using (job_id)
        WHERE job_hash = :job_hash"""
        res = DB.session.execute(q, {"job_hash": str(task.hash)}).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged
