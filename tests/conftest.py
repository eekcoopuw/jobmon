from builtins import str

import os
import pytest
import pwd
import requests
import shutil
import uuid
import socket
import logging
import re
import sys

from datetime import datetime
from sqlalchemy.exc import ProgrammingError
from time import sleep

from cluster_utils.ephemerdb import create_ephemerdb

logger = logging.getLogger(__name__)


def unpack_conn_str(conn_str):
    pattern = ("mysql://(?P<user>.*):(?P<pass>.*)"
               "@(?P<host>.*):(?P<port>.*)/(?P<db>.*)")
    result = re.search(pattern, conn_str)
    return result.groupdict()


@pytest.fixture(scope='session', autouse=True)
def ephemera_conn_str():
    """Note: this function must be placed before the other imports
    because the ephemera db has to be started before any other code
    imports the_server_config
    """
    edb = create_ephemerdb(elevated_privileges=True)
    conn_str = edb.start()
    # use the ephemera db root privileges (root: singularity_root) otherwise
    # you will not see changes to the database
    logger.info(f"Database connection {conn_str}")
    print(f"****** Database connection {conn_str}")
    yield conn_str



# NOTE: there are two types of tests that conftest sets up. 1. Using the real
# flask dev server, to allow us to do real testing of connections to the server
# --see real_jqs_jsm, real_dag_id, etc. 2. Using the fake server, called
# TestClient() which allows us to test certain parts of the code without
# spinning up a webserver--see no_requests_jsm_jqs. Any test that
# requires connection to more than one service (i.e JQS and JSM) requires that
# we use the real dev server.


@pytest.fixture(scope='session')
def test_session_config(ephemera_conn_str):
    db_conn_dict = unpack_conn_str(ephemera_conn_str)
    cfg = {
        "JOBMON_HOST": socket.gethostname(),
        "JOBMON_PORT": "6789",
        "DB_HOST": db_conn_dict["host"],
        "DB_PORT": db_conn_dict["port"],
        "DB_USER": db_conn_dict["user"],
        "DB_PASS": db_conn_dict["pass"],
        "DB_NAME": db_conn_dict["db"]
    }
    return cfg


@pytest.fixture(autouse=True)
def env_var(monkeypatch, test_session_config):
    """These two env variables are what tell the configs that we're running
    tests,
    not production code
    """
    from jobmon.client import shared_requester, client_config
    from jobmon.client.connection_config import ConnectionConfig

    monkeypatch.setenv("JOBMON_HOST", test_session_config["JOBMON_HOST"])
    monkeypatch.setenv("JOBMON_PORT", test_session_config["JOBMON_PORT"])
    monkeypatch.setenv("DB_HOST", test_session_config["DB_HOST"])
    monkeypatch.setenv("DB_PORT", test_session_config["DB_PORT"])
    monkeypatch.setenv("DB_USER", test_session_config["DB_USER"])
    monkeypatch.setenv("DB_PASS", test_session_config["DB_PASS"])
    monkeypatch.setenv("DB_NAME", test_session_config["DB_NAME"])

    cc = ConnectionConfig(host=test_session_config["JOBMON_HOST"],
                          port=test_session_config["JOBMON_PORT"])
    monkeypatch.setattr(shared_requester, 'url', cc.url)
    monkeypatch.setattr(client_config, 'heartbeat_interval', 10)
    monkeypatch.setattr(client_config, 'report_by_buffer', 2.1)
    monkeypatch.setattr(client_config, 'reconciliation_interval', 5)


@pytest.fixture(scope='function')
def local_flask_app(env_var):
    """Sets up the in-process flask app and initializes its database
    connections"""

    from jobmon.server import create_app
    app = create_app()

    # The init_app call sets up database connections
    from jobmon.models import DB

    # Logan Sandar: my assessment is that DB.init_app() should not be necessary
    # because it occurs in create_app() and I think DB has global state.
    # DB.init_app(app)
    yield {'app': app, 'DB': DB}


def create_database_if_needed(app, DB):
    """If the database tables do not exist then create it. The test is
    whether the Workflow table exists."""
    from jobmon.models import database_loaders
    database_exists = False
    with app.app_context():
        try:
            DB.session.execute("""SELECT * FROM workflow""")
            database_exists = True
        except ProgrammingError:
            # ProgrammingError will be thrown if that table does not exist
            pass

        if not database_exists:
            database_loaders.create_job_db(DB)
            database_loaders.load_default_statuses(DB)
            database_loaders.load_attribute_types(DB)
            DB.session.commit()


@pytest.fixture(scope='function')
def db_cfg(local_flask_app):
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh
    """
    from jobmon.models import database_loaders

    app = local_flask_app["app"]
    DB = local_flask_app["DB"]

    create_database_if_needed(app, DB)

    yield {'app': app, 'DB': DB}

    with app.app_context():
        database_loaders.clean_job_db(DB)


@pytest.fixture(scope='session')
def real_jsm_jqs(test_session_config):
    """This starts the flask dev server in separate processes"""
    import multiprocessing as mp
    from tests.run_services import run_web_service

    # spawn ensures that no attributes are copied to the new process. Python
    # starts from scratch
    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_web_service, args=(
        test_session_config["JOBMON_PORT"],
        test_session_config["DB_HOST"],
        test_session_config["DB_PORT"],
        test_session_config["DB_USER"],
        test_session_config["DB_PASS"],
        test_session_config["DB_NAME"],
    ))
    p1.start()

    # Wait for it to be up
    status = 404
    count = 0
    max_tries = 60
    while not status == 200 and count < max_tries:
        try:
            count += 1
            r = requests.get('http://0.0.0.0:{port}'.
                             format(port=test_session_config["JOBMON_PORT"]))
            status = r.status_code
        except Exception:
            # Connection failures land here
            # Safe to catch all because there is a max retry
            pass
        # sleep outside of try block!
        sleep(3)

    if count >= max_tries:
        raise TimeoutError(
            f"Out-of-process jsm and jqs services did not answer after "
            f"{count} attempts, probably failed to start.")
    yield

    p1.terminate()


@pytest.fixture(scope='session')
def jsm_jqs(test_session_config):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server import ServerConfig
    from jobmon.server import create_app

    config = ServerConfig.from_defaults()
    config.db_host = test_session_config["DB_HOST"]
    config.db_port = test_session_config["DB_PORT"]
    config.db_user = test_session_config["DB_USER"]
    config.db_pass = test_session_config["DB_PASS"]
    config.db_name = test_session_config["DB_NAME"]
    app = create_app(config)

    app.config['TESTING'] = True
    client = app.test_client()
    yield client, client


def get_flask_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if 'application/json' in response.headers.get('Content-Type'):
        content = response.json
    elif 'text/html' in response.headers.get('Content-Type'):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope='function')
def no_requests_jsm_jqs(monkeypatch, jsm_jqs):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon.client import requester
    jsm_client, jqs_client = jsm_jqs

    def get_jqs(url, params, headers):
        url = "/" + url.split('/')[-1]
        return jqs_client.get(path=url, query_string=params, headers=headers)
    monkeypatch.setattr(requests, 'get', get_jqs)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)

    def post_jsm(url, json, headers):
        url = "/" + url.split('/')[-1]
        return jsm_client.post(url, json=json, headers=headers)
    monkeypatch.setattr(requests, 'post', post_jsm)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)


@pytest.fixture
def simple_workflow(real_jsm_jqs, db_cfg):
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow

    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    wfa = "my_simple_dag"
    workflow = Workflow(wfa, interrupt_on_error=False)
    workflow.add_tasks([t1, t2, t3])
    workflow.execute()
    return workflow


@pytest.fixture(scope='function')
def dag_id(no_requests_jsm_jqs, db_cfg):
    """This uses the test_client to create a dag in the db and return the
    dag_id
    """
    import random
    from jobmon.client import shared_requester

    rc, response = shared_requester.send_request(
        app_route='/task_dag',
        message={'name': 'test dag', 'user': 'test user',
                 'dag_hash': 'test_{}'.format(random.randint(1, 1000)),
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    yield response['dag_id']


@pytest.fixture(scope='function')
def real_dag_id(real_jsm_jqs, db_cfg):
    """This uses the real Flask dev server to create a dag in the db and
    return the dag_id
    """
    import random
    from jobmon.client import shared_requester

    rc, response = shared_requester.send_request(
        app_route='/task_dag',
        message={'name': 'test dag', 'user': 'test user',
                 'dag_hash': 'test_{}'.format(random.randint(1, 1000)),
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    yield response['dag_id']


@pytest.fixture(scope='module')
def tmp_out_dir():
    """This creates a new tmp_out_dir for every module"""
    u = uuid.uuid4()
    user = pwd.getpwuid(os.getuid()).pw_name
    output_root = ('/ihme/scratch/users/{user}/tests/jobmon/'
                   '{uuid}'.format(user=user, uuid=u))
    yield output_root
    shutil.rmtree(output_root, ignore_errors=True)


@pytest.fixture(scope='function')
def job_list_manager_sub(dag_id):
    """This creates a job_list_manager that uses the Sequential Executor, does
    not start the JobInstanceFactory or JobReconciler threads, and does
    not interrupt on error
    """
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager
    jlm = JobListManager(dag_id, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge(real_dag_id, tmpdir_factory):
    """This creates a job_list_manager that uses the SGEExecutor, does
    start the JobInstanceFactory and JobReconciler threads, and does not
    interrupt on error. It has short reconciliation intervals so that the
    tests run faster than in production.
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_jenkins')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=True,
                         job_instantiation_interval=1,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def dag(db_cfg, no_requests_jsm_jqs, request):
    """This is a fixture for dag creation that uses the test_client,
    so that the dags' JobInstanceFactory and JobInstanceReconcilers get
    cleaned up after each test
    """
    from jobmon.client.swarm.workflow.task_dag import TaskDag
    dag = TaskDag(name=request.node.name, interrupt_on_error=False)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


@pytest.fixture(scope='function')
def real_dag(db_cfg, real_jsm_jqs, request):
    """"This is a fixture for dag creation that uses the real Flask dev server
    so that the dags' JobInstanceFactory and JobInstanceReconcilers get
    cleaned up after each test
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.workflow.task_dag import TaskDag
    executor = SGEExecutor()
    dag = TaskDag(name=request.node.name, executor=executor,
                  interrupt_on_error=False)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


@pytest.fixture(scope='function')
def dag_factory(db_cfg, real_jsm_jqs, request):
    """This is a fixture for creation of lots dag creation that uses the real
    Flask dev server, so that the dags' JobInstanceFactory and
    JobInstanceReconcilers get cleaned up after each test
    """
    from jobmon.client.swarm.workflow.task_dag import TaskDag
    dags = []

    def factory(executor):
        dag = TaskDag(name=request.node.name, executor=executor,
                      interrupt_on_error=False)
        dags.append(dag)
        return dag
    yield factory

    for dag in dags:
        if dag.job_list_manager:
            dag.job_list_manager.disconnect()


@pytest.fixture(autouse=True)
def execution_test_script_perms():
    executed_files = ['executor_args_check.py', 'simple_R_script.r',
                      'simple_stata_script.do', 'memory_usage_array.py',
                      'remote_sleep_and_write.py']
    if sys.version_info.major == 3:
        perms = int("0o755", 8)
    else:
        perms = int("0755", 8)
    path = os.path.dirname(os.path.realpath(__file__))
    shell_path = os.path.join(path, 'shellfiles/')
    files = os.listdir(shell_path)
    os.chmod(shell_path, perms)
    for file in files:
        try:
            os.chmod(f'{shell_path}{file}', perms)
        except Exception as e:
            raise e
    for file in executed_files:
        try:
            os.chmod(f'{path}/{file}', perms)
        except Exception as e:
            raise e



