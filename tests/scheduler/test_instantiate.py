import pytest

from jobmon.client import BashTask, Workflow
from jobmon.execution.scheduler.executor_job import ExecutorJob
from jobmon.models.attributes.constants import qsub_attribute


def test_instantiate_queued_jobs(db_cfg, client_env, sequential_scheduler):

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = Workflow("my_simple_dag", executor_class="SequentialExecutor",
                        seconds_until_timeout=1)
    workflow.add_tasks([t1])

    with pytest.raises(RuntimeError):
        workflow.run()

    sequential_scheduler.instantiate_queued_jobs()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT job_instance.status
        FROM job_instance
        JOIN job using (job_id)
        WHERE job_hash = :job_hash"""
        res = DB.session.execute(sql, {"job_hash": t1.hash}).fetchone()
        DB.session.commit()
    assert res[0] == "D"


def test_n_queued(db_cfg, client_env, dummy_scheduler):

    dummy_scheduler.config.n_queued = 3

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)

    workflow = Workflow("foo", seconds_until_timeout=1,
                        executor_class="DummyExecutor")
    workflow.add_tasks(tasks)
    with pytest.raises(RuntimeError):
        workflow.run()

    # comparing results and times of old query vs new query
    rc, response = workflow.requester.send_request(
        app_route='/queued_jobs/1000',
        message={},
        request_type='get')
    all_jobs = [
        ExecutorJob.from_wire(j, dummy_scheduler.executor.__class__.__name__,
                              workflow.requester)
        for j in response['job_dcts']]

    # now new query that should only return 3 jobs
    select_jobs = dummy_scheduler._get_jobs_queued_for_instantiation()

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20
    for i in range(3):
        assert select_jobs[i].job_id == (i + 1)


@pytest.mark.parametrize('sge', [qsub_attribute.NO_EXEC_ID,
                                 qsub_attribute.UNPARSABLE])
def test_no_executor_id(db_cfg, client_env, dummy_scheduler, monkeypatch, sge):
    t1 = BashTask("echo 2", executor_class="DummyExecutor")
    workflow = Workflow("my_simple_dag", executor_class="DummyExecutor",
                        seconds_until_timeout=1)
    workflow.add_tasks([t1])

    with pytest.raises(RuntimeError):
        workflow.run()

    def mock_execute(*args, **kwargs):
        return sge
    monkeypatch.setattr(dummy_scheduler.executor, "execute", mock_execute)

    dummy_scheduler.instantiate_queued_jobs()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT job_instance.status
        FROM job_instance
        JOIN job using (job_id)
        WHERE job_hash = :job_hash"""
        res = DB.session.execute(sql, {"job_hash": t1.hash}).fetchone()
        DB.session.commit()
    assert res[0] == "W"
