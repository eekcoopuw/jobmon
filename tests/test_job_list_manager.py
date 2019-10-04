import pytest
import logging
from subprocess import check_output
import time

from unittest import mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# import jobmon.client.swarm.executors.sge
from tenacity import stop_after_attempt
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
# from jobmon.models.job_instance_status import JobInstanceStatus
# from jobmon.models.job_instance import JobInstance
from jobmon.client.job_management.job_list_manager import JobListManager
from jobmon.client.workflow.executable_task import ExecutableTask
from jobmon.execution.scheduler.job_instance_scheduler import \
    JobInstanceScheduler
from jobmon.execution.scheduler.executor_job import ExecutorJob


class Task(ExecutableTask):
    """Test version of the Task class for use in this module"""

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


class SingleDagScheduler(JobInstanceScheduler):

    def __init__(self, executor, config, url, dag_id, *args, **kwargs):
        self._time = '2010-01-01 00:00:00'
        self._Session = sessionmaker(bind=create_engine(url))
        self._dag_id = dag_id
        super().__init__(executor, config)

    def _get_jobs_queued_for_instantiation(self):
        session = self._Session()
        try:
            new_time = session.execute(
                "select UTC_TIMESTAMP as time").fetchone()['time']
            session.commit()
            new_time = time.strftime("%Y-%m-%d %H:%M:%S")
            query_time = self._time
            self._time = new_time

            query = """
                SELECT
                    job.*
                FROM
                    job
                WHERE
                    job.dag_id = :dag_id
                    AND job.status = :job_status
                    AND job.status_date >= :last_sync
                ORDER BY job.job_id
                LIMIT :n_queued_jobs"""
            jobs = session.query(Job).from_statement(text(query)).params(
                dag_id=self._dag_id,
                job_status=JobStatus.QUEUED_FOR_INSTANTIATION,
                last_sync=query_time,
                n_queued_jobs=int(self.config.n_queued)
            ).all()
            session.commit()
            jobs = [j.to_wire_as_executor_job() for j in jobs]
            jobs = jobs = [
                ExecutorJob.from_wire(j, self.executor.__class__.__name__,
                                      self.requester)
                for j in jobs]
        finally:
            session.close()
        return jobs


def sequential_scheduler_instance(host, port, reconciliation_interval,
                                  heartbeat_interval, report_by_buffer, url,
                                  dag_id):
    from jobmon.execution.scheduler.execution_config import ExecutionConfig
    from jobmon.execution.strategies.sequential import SequentialExecutor

    cfg = ExecutionConfig.from_defaults()
    cfg.host = host
    cfg.port = port
    cfg.reconciliation_interval = reconciliation_interval
    cfg.heartbeat_interval = heartbeat_interval
    cfg.report_by_buffer = report_by_buffer

    executor = SequentialExecutor()
    return SingleDagScheduler(executor, cfg, url, dag_id)


@pytest.fixture(scope='function')
def sequential_scheduler_jlm(real_jsm_jqs, monkeypatch, db_cfg, real_dag_id,
                             client_env):
    # modify global config before importing anything else
    from jobmon import config
    config.jobmon_server_sqdn = real_jsm_jqs["JOBMON_HOST"]
    config.jobmon_service_port = real_jsm_jqs["JOBMON_PORT"]

    # build sequential scheduler
    from jobmon.execution.scheduler.execution_config import ExecutionConfig
    from jobmon.execution.strategies.sequential import SequentialExecutor
    cfg = ExecutionConfig.from_defaults()
    cfg.reconciliation_interval = 4
    cfg.heartbeat_interval = 2
    cfg.report_by_buffer = 2.1
    executor = SequentialExecutor()
    scheduler = SingleDagScheduler(
        executor, cfg, db_cfg["server_config"].conn_str, real_dag_id)

    yield scheduler, JobListManager(real_dag_id)
    scheduler.stop()
    config.jobmon_server_sqdn = None
    config.jobmon_service_port = None


def test_sync(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    now = jlm.last_sync
    assert now is not None

    # This job will intentionally fail
    job = jlm.bind_task(
        Task(command='fizzbuzz', name='bar',
             executor_class="SequentialExecutor"))
    # create job instances
    jlm.adjust_resources_and_queue(job)
    jid = scheduler.instantiate_queued_jobs()
    assert jid

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs. must wait at least 1
    # second for the times to be different
    time.sleep(1)
    jlm._sync()
    new_now = jlm.last_sync
    assert new_now > now
    assert len(jlm.all_error) > 0


def test_invalid_command(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    job = jlm.bind_task(Task(command='foo', name='bar',
                             executor_class="SequentialExecutor"))
    assert len(jlm.active_jobs) == 0

    jlm.adjust_resources_and_queue(job)
    assert len(jlm.active_jobs) == 1
    assert len(jlm.all_error) == 0

    scheduler.instantiate_queued_jobs()
    jlm._sync()
    assert len(jlm.all_error) > 0


def test_valid_command(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    job = jlm.bind_task(Task(command='ls', name='baz',
                             executor_class="SequentialExecutor"))
    assert len(jlm.active_jobs) == 0
    assert len(jlm.all_done) == 0

    jlm.adjust_resources_and_queue(job)
    assert len(jlm.active_jobs) == 1

    scheduler.instantiate_queued_jobs()
    jlm._sync()
    assert len(jlm.all_done) > 0


def test_blocking_update_timeout(client_env, real_dag_id):
    jlm = JobListManager(real_dag_id)
    job = jlm.bind_task(
        Task(command="sleep 3", name="foobarbaz",
             executor_class="SequentialExecutor"))
    jlm.adjust_resources_and_queue(job)

    with pytest.raises(RuntimeError) as error:
        jlm.block_until_any_done_or_error(timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_server_502(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    """
    GBDSCI-1553

    We should be able to automatically retry if server returns 5XX
    status code. If we exceed retry budget, we should raise informative error
    """
    err_response = (
        502,
        b'<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body '
        b'bgcolor="white">\r\n<center><h1>502 Bad Gateway</h1></center>\r\n'
        b'<hr><center>nginx/1.13.12</center>\r\n</body>\r\n</html>\r\n'
    )
    good_response = (
        200,
        {'job_dcts': [], 'time': '2019-02-21 17:40:07'}
    )

    job = jlm.bind_task(Task(command='ls', name='baz', num_cores=1))
    jlm.adjust_resources_and_queue(job)
    scheduler.instantiate_queued_jobs()

    # mock requester.get_content to return 2 502s then 200
    with mock.patch('jobmon.requester.get_content') as m:
        # Docs: If side_effect is an iterable then each call to the mock
        # will return the next value from the iterable
        m.side_effect = [err_response] * 2 + \
            [good_response] + [err_response] * 2

        jlm.get_job_statuses()  # fails at first

        # should have retried twice + one success
        retrier = jlm.requester.send_request.retry
        assert retrier.statistics['attempt_number'] == 3

        # if we end up stopping we should get an error
        with pytest.raises(RuntimeError, match='Status code was 502'):
            retrier.stop = stop_after_attempt(1)
            jlm.get_job_statuses()


def test_ji_unknown_state(sequential_scheduler_jlm):
    """should try to log a report by date after being set to the U state and
    fail"""
    scheduler, jlm = sequential_scheduler_jlm

    job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
                             executor_class="SequentialExecutor"))
    jlm.adjust_resources_and_queue(job)
    jids = scheduler.instantiate_queued_jobs()

    jlm._sync()
    resp = query_till_running(db_cfg)
    while resp.status != 'R':
        resp = query_till_running(db_cfg)
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job_instance
            SET status = 'U'
            WHERE job_instance_id = {}""".format(jids[0]))
        DB.session.commit()
    exec_id = resp.executor_id
    exit_status = None
    tries = 1
    while exit_status is None and tries < 10:
        try:
            exit_status = check_output(
                f"qacct -j {exec_id} | grep exit_status",
                shell=True, universal_newlines=True)
        except Exception:
            tries += 1
            sleep(3)
    # 9 indicates sigkill signal was sent as expected
    assert '9' in exit_status


# def test_context_args(jlm_sge_no_daemon, db_cfg, caplog):
#     caplog.set_level(logging.DEBUG)

#     jlm = jlm_sge_no_daemon
#     jif = jlm.job_instance_factory

#     job = jlm.bind_task(
#         Task(command="sge_foobar",
#              name="test_context_args", num_cores=2, m_mem_free='4G',
#              max_runtime_seconds='1000',
#              context_args={'sge_add_args': '-a foo'}))

#     jlm.adjust_resources_and_queue(job)
#     jif.instantiate_queued_jobs()

#     assert "-a foo" in caplog.text
