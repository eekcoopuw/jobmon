import pytest
from threading import Thread
from sqlalchemy.exc import IntegrityError

from jobmon import database
from jobmon import config
from jobmon.job_query_server import JobQueryServer
from jobmon.job_state_manager import JobStateManager

from .ephemerdb import EphemerDB


@pytest.fixture(scope='module')
def db():

    db = EphemerDB()
    conn_str = db.start()
    print(conn_str)
    cfg = config.config
    cfg.conn_str = conn_str

    database.create_job_db()
    try:
        with database.session_scope() as session:
            database.load_default_statuses(session)
    except IntegrityError:
        pass

    jsm = JobStateManager(cfg.jm_rep_conn.port, cfg.jm_pub_conn.port)

    jqs = JobQueryServer(cfg.jqs_rep_conn.port)

    t1 = Thread(target=jsm.listen)
    t1.daemon = True
    t1.start()
    t2 = Thread(target=jqs.listen)
    t2.daemon = True
    t2.start()
    yield jsm, jqs
    jsm.stop_listening()
    jqs.stop_listening()
    database.Session.close_all()
    database.engine.dispose()


@pytest.fixture(scope='module')
def dag_id(db):
    jsm = JobStateManager()
    rc, dag_id = jsm.add_job_dag('test_dag', 'test_user')
    yield dag_id
