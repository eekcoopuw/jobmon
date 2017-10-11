import logging
import pytest
from threading import Thread
from sqlalchemy.exc import IntegrityError

from jobmon.config import config
from jobmon import database
from jobmon.job_query_server import JobQueryServer
from jobmon.job_state_manager import JobStateManager

from .ephemerdb import EphemerDB


@pytest.fixture(scope='module')
def db_cfg():

    edb = EphemerDB()
    conn_str = edb.start()
    config.conn_str = conn_str

    # The config has to be reloaded to use the EphemerDB
    database.recreate_engine()
    database.create_job_db()
    try:
        with database.session_scope() as session:
            database.load_default_statuses(session)
    except IntegrityError:
        pass

    yield config

    database.Session.close_all()
    database.engine.dispose()
    edb.stop()


@pytest.fixture(scope='module')
def jsm_jqs(db_cfg):
    # logging does not work well with Threads in python < 2.7,
    # see https://docs.python.org/2/library/logging.html
    # Logging has to be set up BEFORE the Thread.
    # Therefore we set up the job_state_manager's console logger here, before we put it in a Thread.
    jsm_logger = logging.getLogger("jobmon.job_state_manager")
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    jsm_logger.addHandler(ch)
    jsm = JobStateManager(db_cfg.jm_rep_conn.port, db_cfg.jm_pub_conn.port)

    jqs = JobQueryServer(db_cfg.jqs_rep_conn.port)

    t1 = Thread(target=jsm.listen)
    t1.daemon = True
    t1.start()
    t2 = Thread(target=jqs.listen)
    t2.daemon = True
    t2.start()

    yield jsm, jqs
    jsm.stop_listening()
    jqs.stop_listening()


@pytest.fixture(scope='module')
def dag_id(jsm_jqs):
    jsm, jqs = jsm_jqs
    rc, dag_id = jsm.add_job_dag('test_dag', 'test_user')
    yield dag_id
