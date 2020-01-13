import time
import pytest

class MockSchedulerProc:

    def is_alive(self):
        return True


def test_unknown_state(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.execution.scheduler.execution_config import \
        ExecutionConfig

    """Creates a job instance, gets an executor id so it can be in submitted
    to the batch executor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""

    # Queue a job
    task = BashTask(command="ls", name="dummyfbb", max_attempts=1,
                    executor_class="DummyExecutor")
    workflow = UnknownWorkflow("foo", seconds_until_timeout=1,
                               executor_class="DummyExecutor")
    workflow.add_task(task)

    # add workflow info to db and then time out.
    workflow._bind()
    wfr = workflow._create_workflow_run()
    cfg = ExecutionConfig.from_defaults()
    cfg.heartbeat_interval = 5
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id, workflow._executor,
                                      cfg)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    # How long we wait for a JI to report it is running before reconciler moves
    # it to error state.
    scheduler.instantiate_queued_tasks()

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job gets moved to lost_track during reconciliation
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = """
        SELECT task_instance.status
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "B"

    # sleep through the report by date
    time.sleep(scheduler.config.heartbeat_interval *
               scheduler.config.report_by_buffer)

    # job will move into lost track because it never logs a heartbeat
    scheduler.reconcile()
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "U"

    # because we only allow 1 attempt job will move to E after job instance
    # moves to U
    wfr._parse_adjusting_done_and_errors(wfr._task_status_updates())
    assert len(wfr.all_error) > 0


def test_log_executor_report_by(db_cfg, client_env, monkeypatch):
    from jobmon.client.execution.strategies import sequential
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    # patch unwrap from sequential so the command doesn't execute
    def mock_unwrap(*args, **kwargs):
        pass
    monkeypatch.setattr(sequential, "unwrap", mock_unwrap)

    task = BashTask(command="sleep 5", name="heartbeat_sleeper", num_cores=1,
                    max_runtime_seconds=500)
    workflow = UnknownWorkflow("foo", seconds_until_timeout=1,
                               executor_class="SequentialExecutor")
    workflow.add_task(task)

    # add workflow info to db and then time out.
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id, workflow._executor)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    # instantiate the job and then log a report by
    scheduler.instantiate_queued_tasks()
    scheduler._log_executor_report_by()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.submitted_date, task_instance.report_by_date
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
        res = DB.session.execute(sql, {"task_id": str(task.task_id)}
                                 ).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged
