import pytest
import zmq
from time import sleep

from jobmon import config, database, models
from jobmon.job_instance_factory import execute_batch_dummy
from jobmon.job_list_manager import JobListManager
from jobmon.job_state_manager import JobStateManager


@pytest.fixture(scope='module')
def subscriber(dag_id):
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, str(dag_id))
    sub.connect("tcp://localhost:{}".format(config.jm_pub_conn.port))
    return sub
    sub.close()


@pytest.fixture(scope='function')
def job_list_manager(dag_id):
    jlm = JobListManager(dag_id)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_d(dag_id):
    jlm = JobListManager(dag_id, start_daemons=True)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    jlm = JobListManager(dag_id, executor=execute_batch_dummy)
    yield jlm
    jlm.disconnect()


def test_invalid_command(db, dag_id, subscriber, job_list_manager):
    job_id = job_list_manager.create_job('foo', 'bar')
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.queue_job(job_id)
    with database.session_scope() as session:
        job_list_manager._sync(session)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0

    job_list_manager.job_inst_factory.instantiate_queued_jobs()
    with database.session_scope() as session:
        job_list_manager._sync(session)
    assert len(job_list_manager.all_error) > 0


def test_valid_command(db, dag_id, subscriber, job_list_manager):
    job_id = job_list_manager.create_job('ls', 'baz')
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0
    assert len(job_list_manager.all_done) == 0

    job_list_manager.queue_job(job_id)
    with database.session_scope() as session:
        job_list_manager._sync(session)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_inst_factory.instantiate_queued_jobs()
    with database.session_scope() as session:
        job_list_manager._sync(session)
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(db, dag_id, job_list_manager_d):
    job_id = job_list_manager_d.create_job("some new job", "foobar")
    job_list_manager_d.queue_job(job_id)
    sleep(3)  # Give some time for the job to get to the executor
    errors = job_list_manager_d.get_new_errors()
    assert len(errors) == 1


def test_daemon_valid_command(db, dag_id, job_list_manager_d):
    job_id = job_list_manager_d.create_job("ls", "foobarbaz")
    job_list_manager_d.queue_job(job_id)
    sleep(3)  # Give some time for the job to get to the executor
    done = job_list_manager_d.get_new_done()
    assert len(done) == 1


def test_sge_valid_command(db, dag_id, job_list_manager_sge):
    job_id = job_list_manager_sge.create_job("ls", "sgefbb")
    job_list_manager_sge.queue_job(job_id)
    job_list_manager_sge.job_inst_factory.instantiate_queued_jobs()
    with database.session_scope() as session:
        job_list_manager_sge._sync(session)
    assert (job_list_manager_sge.job_statuses[job_id] ==
            models.JobStatus.INSTANTIATED)
