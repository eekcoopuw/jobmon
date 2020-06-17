import pytest

from jobmon.models.constants import qsub_attribute


class MockSchedulerProc:

    def is_alive(self):
        return True


def test_instantiate_queued_jobs(db_cfg, client_env):
    """tests that a task can be instantiated and run and log done"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = UnknownWorkflow("test_instantiate_queued_jobs",
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=1)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.status
        FROM task_instance
        WHERE task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": t1.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "D"


def test_n_queued(db_cfg, client_env):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.execution.scheduler.execution_config import \
        ExecutionConfig
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.serializers import SerializeExecutorTask

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)

    workflow = UnknownWorkflow("test_n_queued", seconds_until_timeout=1,
                               executor_class="DummyExecutor")
    cfg = ExecutionConfig.from_defaults()
    cfg.n_queued = 3
    workflow.set_executor(executor_class="DummyExecutor", execution_config=cfg)
    workflow.add_tasks(tasks)
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id, workflow._executor,
                                      cfg)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    # comparing results and times of old query vs new query
    rc, response = workflow.requester.send_request(
        app_route=f'/workflow/{workflow.workflow_id}/queued_tasks/1000',
        message={},
        request_type='get')
    all_jobs = [
        SerializeExecutorTask.kwargs_from_wire(j)
        for j in response['task_dcts']]

    # now new query that should only return 3 jobs
    select_jobs = scheduler._get_tasks_queued_for_instantiation()

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20


@pytest.mark.parametrize('sge', [qsub_attribute.NO_EXEC_ID,
                                 qsub_attribute.UNPARSABLE])
def test_no_executor_id(db_cfg, client_env, monkeypatch, sge):
    """test that things move successfully into 'W' state if the executor
    returns the correct id"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    t1 = BashTask("echo 2", executor_class="DummyExecutor")
    workflow = UnknownWorkflow(f"my_simple_dag_{sge}",
                               executor_class="DummyExecutor",
                               seconds_until_timeout=1)
    workflow.add_task(t1)
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id, workflow._executor)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    def mock_execute(*args, **kwargs):
        return sge
    monkeypatch.setattr(scheduler.executor, "execute", mock_execute)

    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.status
        FROM task_instance
        WHERE task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": t1.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "W"
