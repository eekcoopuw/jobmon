import functools
import os
import pytest
import pwd
import shutil
import socket
import threading
import uuid
from datetime import datetime

from argparse import Namespace
from sqlalchemy.exc import IntegrityError

from jobmon.bootstrap import install_rcfile


def create_rcfile_dir():
    u = uuid.uuid4()
    user = pwd.getpwuid(os.getuid()).pw_name
    rcdir = ('/ihme/scratch/users/{user}/tests/jobmon/'
             '{uuid}'.format(user=user, uuid=u))
    try:
        os.makedirs(rcdir)
    except:
        pass
    return rcdir


def get_random_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def create_sqlite_rcfile(rcdir):
    args = Namespace()
    args.force = False
    args.file = "{}/jobmonrc".format(rcdir)
    try:
        install_rcfile(args,
                       cfg_dct={"conn_str": "sqlite://",
                                "host": socket.gethostname(),
                                "jsm_port": 5056,
                                "jqs_port": 5058})

        cleanup_rcfile = True
    except FileExistsError:
        # It's OK for now if the rcfile already exists. May need to revisit
        # this once we have a more sensible mechanism for versioning the
        # RCFILEs
        cleanup_rcfile = False
    return args.file, cleanup_rcfile


def bootstrap_tests():
    """The ordering of this script is essential. Bootstrap_tests must be called
       here to force the CONFIG changes to happen before any jobmon module
       actually attempts to load the config"""
    global rcdir
    rcdir = create_rcfile_dir()
    global sqlite_rcfile
    global cleanup_sqlite_rcfile
    sqlite_rcfile, cleanup_sqlite_rcfile = create_sqlite_rcfile(rcdir)
    os.environ["JOBMON_CONFIG"] = sqlite_rcfile


bootstrap_tests()


from jobmon.config import config
from jobmon import database
from jobmon import database_loaders
from jobmon.job_list_manager import JobListManager
from jobmon.services import job_query_server as jqs
from jobmon.services import job_state_manager as jsm
from jobmon.job_instance_factory import execute_sge
from jobmon.workflow.task_dag import TaskDag

from cluster_utils.ephemerdb import create_ephemerdb


@pytest.fixture(autouse=True)
def env_var(monkeypatch, rcfile):
    monkeypatch.setenv("JOBMON_CONFIG", rcfile)


@pytest.fixture(scope='session')
def rcfile_dir():
    yield rcdir
    shutil.rmtree(rcdir)


@pytest.fixture(scope='session')
def rcfile(rcfile_dir):
    yield sqlite_rcfile

    if cleanup_sqlite_rcfile:
        os.remove(os.path.expanduser(sqlite_rcfile))


@pytest.fixture(scope='session')
def session_edb(rcfile):

    edb = create_ephemerdb()
    conn_str = edb.start()
    config.conn_str = conn_str

    # The config has to be reloaded to use the EphemerDB
    database.recreate_engine()
    database_loaders.create_job_db()
    try:
        with database.session_scope() as session:
            database_loaders.load_default_statuses(session)
    except IntegrityError:
        pass

    yield config

    database.Session.close_all()
    database.engine.dispose()
    edb.stop()


@pytest.fixture(scope='function')
def db_cfg(session_edb):

    database_loaders.delete_job_db()
    database_loaders.create_job_db()
    try:
        with database.session_scope() as session:
            database_loaders.load_default_statuses(session)
    except IntegrityError:
        pass

    yield config


@pytest.fixture(scope='module')
def jsm_jqs(session_edb):
    t1 = threading.Thread(target=jsm.flask_thread)
    t1.daemon = True
    t1.start()

    t2 = threading.Thread(target=jqs.flask_thread)
    t2.daemon = True
    t2.start()

    yield

    t1.join()
    t2.join()


@pytest.fixture(scope='module')
def dag_id(jsm_jqs):
    import random
    rc, dag_id = jsm.add_task_dag('test_dag', 'test_user',
                                  'test_{}'.format(random.randint(1, 1000)),
                                  datetime.utcnow())
    yield dag_id


@pytest.fixture(scope='module')
def tmp_out_dir():
    u = uuid.uuid4()
    user = pwd.getpwuid(os.getuid()).pw_name
    output_root = ('/ihme/scratch/users/{user}/tests/jobmon/'
                   '{uuid}'.format(user=user, uuid=u))
    yield output_root
    shutil.rmtree(output_root, ignore_errors=True)


@pytest.fixture(scope='function')
def job_list_manager_sub(dag_id):
    jlm = JobListManager(dag_id, interrupt_on_error=False)
    yield jlm


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id, tmpdir_factory):

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = functools.partial(execute_sge, stderr=elogdir, stdout=ologdir,
                                 project='proj_jenkins')
    executor.__name__ = execute_sge.__name__
    jlm = JobListManager(dag_id, executor=executor, start_daemons=True,
                         interrupt_on_error=False)
    yield jlm


@pytest.fixture(scope='function')
def dag(db_cfg, jsm_jqs, request):
    """Use a fixture for dag creation so that the dags' JobInstanceFactories
    and JobInstanceReconcilers get cleaned up after each test"""
    dag = TaskDag(name=request.node.name, interrupt_on_error=False)
    yield dag
