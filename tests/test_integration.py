import pytest
from multiprocessing import Process

from jobmon import database
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_state_manager import JobStateManager


def listen():
    jsm = JobStateManager(4567)
    jsm.open_socket()
    jsm.listen()


@pytest.fixture(scope='module')
def db():
    database.create_job_db()
    session = database.Session()
    database.load_default_statuses(session)
    session.close()
    yield True
    # database.delete_job_db()


@pytest.fixture(scope='module')
def dag_id():
    jsm = JobStateManager(4567)
    rc, dag_id = jsm.add_job_dag('test_dag', 'test_user')
    return dag_id


@pytest.fixture(scope='module')
def job_state_manager():
    proc = Process(target=listen)
    proc.start()
    yield proc
    proc.terminate()


@pytest.fixture(scope='function')
def session():
    session = database.Session()
    yield session
    session.close()


def test_happy_path(db, dag_id, job_state_manager, session):
    jf = JobFactory(dag_id)
    job_id = jf.create_job('foo', 'bar')
    njobs0 = jf._get_instantiated_not_done_not_fatal(session)
    assert len(njobs0) == 0

    jf.queue_job(job_id)
    njobs1 = jf._get_instantiated_not_done_not_fatal(session)
    assert len(njobs1) == 1

    jif = JobInstanceFactory(dag_id)
    jif.instantiate_queued_jobs()
