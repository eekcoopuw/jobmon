import pytest
from threading import Thread

from jobmon import config, database
from jobmon.job_query_server import JobQueryServer
from jobmon.job_state_manager import JobStateManager


@pytest.fixture(scope='session')
def db():
    database.create_job_db()
    with database.session_scope() as session:
        database.load_default_statuses(session)

    jsm = JobStateManager(config.jm_rep_conn.port, config.jm_pub_conn.port)

    jqs = JobQueryServer(config.jqs_rep_conn.port)

    t1 = Thread(target=jsm.listen)
    t1.daemon = True
    t1.start()
    t2 = Thread(target=jqs.listen)
    t2.daemon = True
    t2.start()
    yield
    jsm.stop_listening()
    jqs.stop_listening()
    database.Session.close_all()
    database.engine.dispose()


@pytest.fixture(scope='session')
def dag_id(db):
    jsm = JobStateManager()
    rc, dag_id = jsm.add_job_dag('test_dag', 'test_user')
    yield dag_id
