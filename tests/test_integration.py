import pytest
import zmq
from time import sleep
from queue import Empty

from jobmon import database, models
from jobmon.config import config
from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager


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
    jlm = JobListManager(dag_id, executor=execute_sge)
    yield jlm
    jlm.disconnect()


def test_invalid_command(subscriber, job_list_manager):
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


def test_valid_command(subscriber, job_list_manager):
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


def test_daemon_invalid_command(job_list_manager_d):
    job_id = job_list_manager_d.create_job("some new job", "foobar")
    job_list_manager_d.queue_job(job_id)
    sleep(5)  # Give some time for the job to get to the executor
    errors = job_list_manager_d.get_new_errors()
    assert len(errors) == 1


def test_daemon_valid_command(job_list_manager_d):
    job_id = job_list_manager_d.create_job("ls", "foobarbaz")
    job_list_manager_d.queue_job(job_id)
    sleep(5)  # Give some time for the job to get to the executor
    done = job_list_manager_d.get_new_done()
    assert len(done) == 1


def test_blocking_updates(job_list_manager_d):

    # Test 1 job
    job_id = job_list_manager_d.create_job("sleep 2", "foobarbaz")
    job_list_manager_d.queue_job(job_id)
    done = job_list_manager_d.block_until_any_done_or_error()
    assert len(done) == 1
    assert done[0] == (job_id, models.JobStatus.DONE)

    # Test multiple jobs

    job_list_manager_d.get_new_done()  # clear the done queue for this test
    job_list_manager_d.get_new_errors()  # clear the error queue too
    job_id1 = job_list_manager_d.create_job("sleep 1", "foobarbaz1")
    job_id2 = job_list_manager_d.create_job("sleep 1", "foobarbaz2")
    job_id3 = job_list_manager_d.create_job("not a command", "foobarbaz2")
    job_list_manager_d.queue_job(job_id1)
    job_list_manager_d.queue_job(job_id2)
    job_list_manager_d.queue_job(job_id3)
    sleep(3)
    done, errors = job_list_manager_d.block_until_no_instances(
        raise_on_any_error=False)
    assert len(done) == 2
    assert len(errors) == 1
    assert set(done) == set([job_id1, job_id2])
    assert set(errors) == set([job_id3])


def test_blocking_update_timeout(job_list_manager_d):
    job_id = job_list_manager_d.create_job("sleep 3", "foobarbaz")
    job_list_manager_d.queue_job(job_id)
    with pytest.raises(Empty):
        job_list_manager_d.block_until_any_done_or_error(timeout=2)


def test_sge_valid_command(job_list_manager_sge):
    job_id = job_list_manager_sge.create_job("ls", "sgefbb", slots=3,
                                             mem_free=6)
    job_list_manager_sge.queue_job(job_id)
    job_list_manager_sge.job_inst_factory.instantiate_queued_jobs()
    with database.session_scope() as session:
        job_list_manager_sge._sync(session)
    assert (job_list_manager_sge.job_statuses[job_id] ==
            models.JobStatus.INSTANTIATED)
