import pytest
import zmq
from multiprocessing import Process

from jobmon import database
from jobmon.connection_config import ConnectionConfig
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_list_manager import JobListManager
from jobmon.job_state_manager import JobStateManager
from jobmon.requester import Requester


REP_PORT = 4567
PUB_PORT = 5678


def listen():
    jsm = JobStateManager(REP_PORT, PUB_PORT)
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
    jsm = JobStateManager()
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


@pytest.fixture(scope='module')
def subscriber(dag_id):
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, str(dag_id))
    sub.connect("tcp://localhost:{}".format(PUB_PORT))
    return sub


@pytest.fixture(scope='module')
def job_list_manager(dag_id):
    jlm = JobListManager(dag_id)
    jlm._start_job_status_listener()
    return jlm


def test_happy_path(db, dag_id, job_state_manager, session, subscriber,
                    job_list_manager):
    jf = JobFactory(dag_id)
    job_id = jf.create_job('foo', 'bar')
    njobs0 = job_list_manager._get_instantiated_not_done_not_fatal(session)
    assert len(njobs0) == 0

    jf.queue_job(job_id)
    njobs1 = job_list_manager._get_instantiated_not_done_not_fatal(session)
    assert len(njobs1) == 1

    jif = JobInstanceFactory(dag_id)
    job_instance_ids = jif.instantiate_queued_jobs()

    cc = ConnectionConfig('localhost', 4567)
    req = Requester(cc)
    re_running = req.send_request({
        'action': 'log_running',
        'kwargs': {'job_instance_id': job_instance_ids[0]}
    })
    re_done = req.send_request({
        'action': 'log_done',
        'kwargs': {'job_instance_id': job_instance_ids[0]}
    })
    print("running_rc: ", re_running)
    print("done_rc: ", re_done)
    print("subscriber receieved: ", subscriber.recv())
    new_done = job_list_manager.get_new_done()
    print(new_done)
    print(job_list_manager.all_done)
    assert len(new_done) > 0
    job_list_manager._stop_job_status_listener()

    re_invalid_done = req.send_request({
        'action': 'log_done',
        'kwargs': {'job_instance_id': job_instance_ids[0]}
    })
    print("invalid_done_rc: ", re_invalid_done)
