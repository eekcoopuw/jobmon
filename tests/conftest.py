import functools
import os
import pytest
import pwd
import shutil
import socket
import uuid
from datetime import datetime

from argparse import Namespace
from sqlalchemy.exc import IntegrityError
from threading import Thread

from jobmon.config import GlobalConfig, config
from jobmon import database
from jobmon.cli import install_rcfile
from jobmon.job_list_manager import JobListManager
from jobmon.services.job_query_server import JobQueryServer
from jobmon.services.job_state_manager import JobStateManager
from jobmon.job_instance_factory import execute_sge
from jobmon.workflow.task_dag import TaskDag

from cluster_utils.ephemerdb import create_ephemerdb


@pytest.fixture(autouse=True)
def env_var(monkeypatch, rcfile):
    monkeypatch.setenv("JOBMON_CONFIG", rcfile)


@pytest.fixture(scope='session')
def rcfile_dir():
    u = uuid.uuid4()
    user = pwd.getpwuid(os.getuid()).pw_name
    rcdir = ('/ihme/scratch/users/{user}/tests/jobmon/'
             '{uuid}'.format(user=user, uuid=u))
    try:
        os.makedirs(rcdir)
    except:
        pass
    yield rcdir
    shutil.rmtree(rcdir)


@pytest.fixture(scope='session')
def rcfile(rcfile_dir):
    args = Namespace()
    args.force = False
    args.file = "{}/jobmonrc".format(rcfile_dir)
    try:
        # these port numbers don't need to match the deploy ports,
        # as they're for testing only
        install_rcfile(args,
                       cfg_dct={"conn_str": "sqlite://",
                                "jsm_host": socket.gethostname(),
                                "jqs_host": socket.gethostname(),
                                "jsm_rep_port": 3456,
                                "jsm_pub_port": 3457,
                                "jqs_port": 3458})
        cleanup_rcfile = True
    except FileExistsError:
        # It's OK for now if the rcfile already exists. May need to revisit
        # this once we have a more sensible mechanism for versioning the
        # RCFILEs
        cleanup_rcfile = False

    opts_dct = GlobalConfig.get_file_opts(args.file)
    config.apply_opts_dct(opts_dct)
    yield args.file

    if cleanup_rcfile:
        os.remove(os.path.expanduser(args.file))


@pytest.fixture(scope='session')
def session_edb(rcfile):

    edb = create_ephemerdb()
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


@pytest.fixture(scope='function')
def db_cfg(session_edb):

    database.delete_job_db()
    database.create_job_db()
    try:
        with database.session_scope() as session:
            database.load_default_statuses(session)
    except IntegrityError:
        pass

    yield config


@pytest.fixture(scope='module')
def jsm_jqs(session_edb):
    # logging does not work well with Threads in python < 2.7,
    # see https://docs.python.org/2/library/logging.html
    # Logging has to be set up BEFORE the Thread.
    # Therefore we set up the job_state_manager's console logger here, before
    # we put it in a Thread.
    jsm = JobStateManager(session_edb.jm_rep_conn.port,
                          session_edb.jm_pub_conn.port)

    jqs = JobQueryServer(session_edb.jqs_rep_conn.port)

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
    import random
    jsm, jqs = jsm_jqs
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
    jlm._start_job_status_listener()
    yield jlm
    jlm.disconnect()


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
    jlm.disconnect()


@pytest.fixture(scope='function')
def dag(db_cfg, jsm_jqs, request):
    """Use a fxiture for dag creation so that the dags' JobInstanceFactories
    and JobInstanceReconcilers get cleaned up after each test"""
    dag = TaskDag(name=request.node.name, interrupt_on_error=False)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()
