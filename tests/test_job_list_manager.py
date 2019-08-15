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
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask

from tests.timeout_and_skip import timeout_and_skip

from functools import partial


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
def job_list_manager_d(real_dag_id):
    """Quick job_instantiation_interval for quick tests"""
    jlm = JobListManager(real_dag_id, start_daemons=True,
                         job_instantiation_interval=1)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge_no_daemons(real_dag_id):
    """This fixture starts a JobListManager using the SGEExecutor, but without
    running JobInstanceFactory or JobReconciler in daemonized threads.
    Quick job_instantiation_interval for quick tests.
    """
    executor = SGEExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         job_instantiation_interval=1)
    yield jlm
    jlm.disconnect()


def get_presumed_submitted_or_running(DB, dag_id):
    job_instances = DB.session.query(JobInstance).\
        filter_by(dag_id=dag_id).\
        filter(JobInstance.status.in_([
                JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                JobInstanceStatus.RUNNING])).\
        all()  # noqa: E711
    DB.session.commit()
    return job_instances


def test_sync(job_list_manager_sge_no_daemons, db_cfg):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    now = job_list_manager_sge.last_sync
    assert now is not None

    # This job will intentionally fail
    job = job_list_manager_sge.bind_task(Task(command='fizzbuzz', name='bar',
                                              mem_free='1G',
                                              max_runtime_seconds='1000',
                                              num_cores=1))
    # create job instances
    job_list_manager_sge.queue_job(job)
    jid = job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()

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
                DB, job_list_manager_sge.dag_id)

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs
    job_list_manager_sge._sync()
    new_now = job_list_manager_sge.last_sync
    assert new_now > now
    assert len(job_list_manager_sge.all_error) > 0


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.queue_job(job.job_id)
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
    job_list_manager.queue_job(job.job_id)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="some new job",
                                            name="foobar", num_cores=1))
    job_list_manager_d.queue_job(job.job_id)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, "foobar", partial(
        daemon_invalid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_invalid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_error) == 1


def test_daemon_valid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="ls", name="foobarbaz",
                                            num_cores=1))
    job_list_manager_d.queue_job(job.job_id)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, "foobarbaz", partial(
        daemon_valid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_valid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_done) == 1


def test_blocking_update_timeout(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="sleep 3",
                                            name="foobarbaz", num_cores=1))
    job_list_manager_d.queue_job(job)

    with pytest.raises(RuntimeError) as error:
        job_list_manager_d.block_until_any_done_or_error(timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sge_valid_command(job_list_manager_sge_no_daemons):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    job = job_list_manager_sge.bind_task(Task(command="ls",
                                              name="sgefbb",
                                              num_cores=3,
                                              max_runtime_seconds='1000',
                                              mem_free='600M'))
    job_list_manager_sge.queue_job(job)
    job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()
    job_list_manager_sge._sync()
    assert (job_list_manager_sge.bound_tasks[job.job_id].status ==
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
    job_list_manager.queue_job(job)
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


def test_job_instance_qsub_error(job_list_manager_sge_no_daemons, db_cfg,
                                 monkeypatch, caplog):
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_qsub_error)
    jlm = job_list_manager_sge_no_daemons
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds='1000', mem_free='600M'))
    jlm.queue_job(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        DB.session.commit()
    assert resp[0].status == 'W'
    assert resp[0].executor_id is None
    assert "Received -99999 meaning the job did not qsub properly, moving " \
           "to 'W' state" in caplog.text


def test_job_instance_bad_qsub_parse(job_list_manager_sge_no_daemons, db_cfg,
                                     monkeypatch, caplog):
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_parse_qsub_resp_error)
    jlm = job_list_manager_sge_no_daemons
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds='1000', mem_free='600M'))
    jlm.queue_job(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        job_info = DB.session.execute("""SELECT * FROM job""").fetchall()
        DB.session.commit()
    assert resp[0].status == 'W'
    assert resp[0].executor_id is None
    assert job_info[0].status == 'F'
    assert "Got response from qsub but did not contain a valid executor_id. " \
           "Using (-33333), and moving to 'W' state" in caplog.text


def test_ji_unknown_state(job_list_manager_sge_no_daemons, db_cfg):
    """should try to log a report by date after being set to the L state and
    fail"""
    jlm = job_list_manager_sge_no_daemons
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
                             num_cores=3, max_runtime_seconds='70',
                             mem_free='600M'))
    jlm.queue_job(job)
    jids = jif.instantiate_queued_jobs()
    jlm._sync()
    resp = query_till_running(db_cfg)
    while resp.status != 'R':
        resp = query_till_running(db_cfg)
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""UPDATE job_instance
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


def query_till_running(db_cfg):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute(
            """SELECT status, executor_id FROM job_instance""").fetchall()[0]
        DB.session.commit()
    return resp


def test_context_args(job_list_manager_sge_no_daemons, db_cfg, caplog):
    caplog.set_level(logging.DEBUG)

    jlm = job_list_manager_sge_no_daemons
    jif = jlm.job_instance_factory

    job = jlm.bind_task(
        Task(command="sge_foobar",
             name="test_context_args", num_cores=2, mem_free='4G',
             max_runtime_seconds='1000',
             context_args={'sge_add_args': '-a foo'}))

    jlm.queue_job(job)
    jif.instantiate_queued_jobs()

    assert "-a foo" in caplog.text
