import pytest
import logging
from subprocess import check_output
from time import sleep
from unittest import mock

import jobmon.client.swarm.executors.sge
from tenacity import stop_after_attempt
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask

from tests.conftest import teardown_db

class Task(ExecutableTask):
    """Test version of the Task class for use in this module"""

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


@pytest.fixture(scope='function')
def job_list_manager(real_dag_id):
    jlm = JobListManager(real_dag_id)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_daemon(real_dag_id):
    """Quick job_instantiation_interval for quick tests"""
    jlm = JobListManager(real_dag_id, start_daemons=True,
                         job_instantiation_interval=1)
    yield jlm
    jlm.disconnect()


def test_sync(db_cfg, jlm_sge_no_daemon):
    teardown_db(db_cfg)
    now = jlm_sge_no_daemon.last_sync
    assert now is not None

    # This job will intentionally fail
    job = jlm_sge_no_daemon.bind_task(
        Task(command='fizzbuzz', name='bar',
             m_mem_free='1G',
             max_runtime_seconds='1000',
             num_cores=1))
    # create job instances
    jlm_sge_no_daemon.adjust_resources_and_queue(job)
    jid = jlm_sge_no_daemon.job_instance_factory.instantiate_queued_jobs()

    def get_presumed_submitted_or_running(DB, dag_id):
        job_instances = DB.session.query(JobInstance).\
            filter_by(dag_id=dag_id).\
            filter(JobInstance.status.in_([
                    JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                    JobInstanceStatus.RUNNING])).\
            all()  # noqa: E711
        DB.session.commit()
        return job_instances

    # check that the job clears the queue by checking the jqs for any
    # submitted or running
    max_sleep = 600  # 10 min max till test fails
    slept = 0
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    while jid and slept <= max_sleep:
        slept += 5
        sleep(5)
        with app.app_context():
            jid = get_presumed_submitted_or_running(
                DB, jlm_sge_no_daemon.dag_id)

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs
    jlm_sge_no_daemon._sync()
    new_now = jlm_sge_no_daemon.last_sync
    assert new_now > now
    assert len(jlm_sge_no_daemon.all_error) > 0
    teardown_db(db_cfg)


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.adjust_resources_and_queue(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0
    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_error) > 0


def test_valid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='ls', name='baz',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0
    assert len(job_list_manager.all_done) == 0
    job_list_manager.adjust_resources_and_queue(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(job_list_manager_daemon):
    job = job_list_manager_daemon.bind_task(
        Task(command="some new job", name="foobar", num_cores=1))
    job_list_manager_daemon.adjust_resources_and_queue(job)

    job_list_manager_daemon._sync()
    return len(job_list_manager_daemon.all_error) == 1


def test_daemon_valid_command(job_list_manager_daemon):
    job = job_list_manager_daemon.bind_task(
        Task(command="ls", name="foobarbaz", num_cores=1))
    job_list_manager_daemon.adjust_resources_and_queue(job)
    job_list_manager_daemon._sync()
    return len(job_list_manager_daemon.all_done) == 1


def daemon_valid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_done) == 1


def test_blocking_update_timeout(job_list_manager_daemon):
    job = job_list_manager_daemon.bind_task(
        Task(command="sleep 3", name="foobarbaz", num_cores=1))
    job_list_manager_daemon.adjust_resources_and_queue(job)

    with pytest.raises(RuntimeError) as error:
        job_list_manager_daemon.block_until_any_done_or_error(timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sge_valid_command(jlm_sge_no_daemon):
    job = jlm_sge_no_daemon.bind_task(
        Task(command="ls", name="sgefbb", num_cores=3,
             max_runtime_seconds='1000', m_mem_free='600M'))
    jlm_sge_no_daemon.adjust_resources_and_queue(job)
    jlm_sge_no_daemon.job_instance_factory.instantiate_queued_jobs()
    jlm_sge_no_daemon._sync()
    assert (jlm_sge_no_daemon.bound_tasks[job.job_id].status ==
            JobStatus.INSTANTIATED)
    print("finishing test_sge_valid_command")


def test_server_502(job_list_manager):
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

    job = job_list_manager.bind_task(Task(command='ls', name='baz',
                                          num_cores=1))
    job_list_manager.adjust_resources_and_queue(job)
    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # mock requester.get_content to return 2 502s then 200
    with mock.patch('jobmon.client.requester.get_content') as m:
        # Docs: If side_effect is an iterable then each call to the mock
        # will return the next value from the iterable
        m.side_effect = [err_response] * 2 + \
            [good_response] + [err_response] * 2

        job_list_manager.get_job_statuses()  # fails at first

        # should have retried twice + one success
        retrier = job_list_manager.requester.send_request.retry
        assert retrier.statistics['attempt_number'] == 3

        # if we end up stopping we should get an error
        with pytest.raises(RuntimeError, match='Status code was 502'):
            retrier.stop = stop_after_attempt(1)
            job_list_manager.get_job_statuses()


class ExpectedException(Exception):
    pass


def mock_qsub_error(cmd, shell=False, universal_newlines=True):
    raise ExpectedException("qsub didnt work")


def mock_parse_qsub_resp_error(cmd, shell=False, universal_newlines=True):
    return ("NO executor id here")


def test_job_instance_qsub_error(db_cfg, jlm_sge_no_daemon, monkeypatch,
                                 caplog):
    teardown_db(db_cfg)
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_qsub_error)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds='1000', m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        DB.session.commit()

    # most recent job is from this test
    assert resp[-1].status == 'W'
    assert resp[-1].executor_id is None
    assert "Received -99999 meaning the job did not qsub properly, moving " \
           "to 'W' state" in caplog.text
    teardown_db(db_cfg)


def test_job_instance_bad_qsub_parse(db_cfg, jlm_sge_no_daemon, monkeypatch,
                                     caplog):
    teardown_db(db_cfg)
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_parse_qsub_resp_error)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds='1000', m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        job_info = DB.session.execute("""SELECT * FROM job""").fetchall()
        DB.session.commit()
    assert resp[-1].status == 'W'
    assert resp[-1].executor_id is None
    assert job_info[-1].status == 'F'
    assert "Got response from qsub but did not contain a valid executor_id. " \
           "Using (-33333), and moving to 'W' state" in caplog.text
    teardown_db(db_cfg)


def query_till_running(db_cfg):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute(
            """SELECT status, executor_id FROM job_instance"""
        ).fetchall()[-1]
        DB.session.commit()
    return resp


def test_ji_unknown_state(db_cfg, jlm_sge_no_daemon):
    """should try to log a report by date after being set to the L state and
    fail"""
    teardown_db(db_cfg)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
                             num_cores=1, max_runtime_seconds=120,
                             m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jids = jif.instantiate_queued_jobs()
    jlm._sync()
    resp = query_till_running(db_cfg)
    count = 0
    while resp.status != 'R' and count < 20:
        resp = query_till_running(db_cfg)
        sleep(10)
        count = count + 1
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
    teardown_db(db_cfg)


def test_ji_kill_self_state(db_cfg, jlm_sge_no_daemon):
    """Job instance should poll for for a job state KILL_SELF and kill
    itself. Basically test_ji_unknown_state but setting K state instead
    """
    teardown_db(db_cfg)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="sleep 60", name="kill_self_task",
                             num_cores=1, max_runtime_seconds=120,
                             m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jids = jif.instantiate_queued_jobs()
    jlm._sync()
    resp = query_till_running(db_cfg)
    count = 0
    while resp.status != 'R' and count < 20:
        resp = query_till_running(db_cfg)
        sleep(10)
        count = count + 1
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job_instance
            SET status = 'K'
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
    teardown_db(db_cfg)


def test_context_args(db_cfg, jlm_sge_no_daemon, caplog):
    teardown_db(db_cfg)
    caplog.set_level(logging.DEBUG)

    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory

    job = jlm.bind_task(
        Task(command="sge_foobar",
             name="test_context_args", num_cores=2, m_mem_free='4G',
             max_runtime_seconds='1000',
             context_args={'sge_add_args': '-a foo'}))

    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()

    assert "-a foo" in caplog.text
    teardown_db(db_cfg)
