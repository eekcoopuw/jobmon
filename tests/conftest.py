from builtins import str

import os
import pytest
import pwd
import shutil
import uuid
import socket
from datetime import datetime
from time import sleep
from sqlalchemy.exc import IntegrityError
from cluster_utils.ephemerdb import create_ephemerdb

import jobmon
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.task_dag import TaskDag
from jobmon.attributes import attribute_database_loaders


# NOTE: there are two types of tests that conftest sets up. 1. Using the real
# flask dev server, to allow us to do real testing of connections to the server
# --see real_jqs_jsm, real_dag_id, etc. 2. Using the fake server, called
# TestClient() which allows us to test certain parts of the code without
# spinning up a webserver--see no_requests_jsm_jqs. Any test that
# requires connection to more than one service (i.e JQS and JSM) requires that
# we use the real dev server.


def get_test_client_config():
    from jobmon.client.config import ClientConfig, \
        derive_jobmon_command_from_env
    if 'the_client_config' not in globals():
        global the_client_config
        the_client_config = ClientConfig(
            jobmon_version=str(jobmon.__version__),
            host=socket.gethostname(),
            jsm_port=5056,
            jqs_port=5058,
            jobmon_command=derive_jobmon_command_from_env())
    return the_client_config


@pytest.fixture(scope='function', autouse=True)
def patch_client_config(monkeypatch):
    from jobmon.client import the_client_config

    monkeypatch.setattr(the_client_config, 'get_the_client_config',
                        get_test_client_config)


def assign_ephemera_conn_str():
    edb = create_ephemerdb()
    conn_str = edb.start()
    return edb, conn_str


def get_test_server_config():
    from jobmon.server.config import ServerConfig

    if 'the_server_config' not in globals():
        global the_server_config
        edb, conn_str = assign_ephemera_conn_str()
        the_server_config = ServerConfig(
            jobmon_version=str(jobmon.__version__),
            conn_str=conn_str,
            slack_token=None,
            default_wf_slack_channel=None,
            default_node_slack_channel=None,
            verbose=False)
        the_server_config.session_edb = edb
    return the_server_config


@pytest.fixture(scope='function', autouse=True)
def patch_server_config(monkeypatch):
    from jobmon.server import the_server_config

    monkeypatch.setattr(the_server_config, 'get_the_server_config',
                        get_test_server_config)


@pytest.fixture(scope="session")
def teardown_edb(request):
    def teardown():
        from jobmon.server.the_server_config import get_the_server_config
        from jobmon.server import database
        database.Session.close_all()
        database.engine.dispose()
        get_the_server_config().session_edb.stop()
    request.addfinalizer(teardown)


@pytest.fixture(scope='function')
def db_cfg():

    from jobmon.server import database_loaders
    from jobmon.server import database
    database_loaders.delete_job_db()
    database_loaders.create_job_db()
    try:
        with database.session_scope() as session:
            database_loaders.load_default_statuses(session)
            attribute_database_loaders.load_attribute_types(session)
    except IntegrityError:
        pass


@pytest.fixture(scope='session')
def real_jsm_jqs():
    import multiprocessing as mp
    from tests.run_services import run_jsm, run_jqs

    the_client_config = get_test_client_config()
    client_cfg_opts = {'host': the_client_config.host,
                       'jsm_port': the_client_config.jsm_port,
                       'jqs_port': the_client_config.jqs_port,
                       'jobmon_version': the_client_config.jobmon_version,
                       'jobmon_command': the_client_config.jobmon_command}

    the_server_config = get_test_server_config()
    server_cfg_opts = {
        'jobmon_version': the_server_config.jobmon_version,
        'conn_str': the_server_config.conn_str,
        'slack_token': the_server_config.slack_token,
        'default_wf_slack_channel': the_server_config.default_wf_slack_channel,
        'default_node_slack_channel':
        the_server_config.default_node_slack_channel,
        'verbose': the_server_config.verbose}

    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_jsm, args=(client_cfg_opts,
                                           server_cfg_opts,))
    p1.start()

    p2 = ctx.Process(target=run_jqs, args=(client_cfg_opts,
                                           server_cfg_opts,))
    p2.start()

    sleep(30)
    yield

    p1.terminate()
    p2.terminate()


@pytest.fixture(scope='session')
def jsm_jqs():
    from jobmon.server.services.job_state_manager.job_state_manager import \
        app as jsm_app
    from jobmon.server.services.job_query_server.job_query_server import app \
        as jqs_app

    jsm_app.config['TESTING'] = True
    jqs_app.config['TESTING'] = True
    jsm_client = jsm_app.test_client()
    jqs_client = jqs_app.test_client()
    yield jsm_client, jqs_client


def get_flask_content(response):
    if 'application/json' in response.headers.get('Content-Type'):
        content = response.json
    elif 'text/html' in response.headers.get('Content-Type'):
        content = response.data
    else:
        content = response.content
    return content


@pytest.fixture(scope='function')
def no_requests_jsm_jqs(monkeypatch, jsm_jqs):
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


@pytest.fixture(scope='function')
def dag_id(no_requests_jsm_jqs, db_cfg):
    import random
    from jobmon.client.requester import Requester
    from jobmon.client.the_client_config import get_the_client_config

    req = Requester(get_the_client_config(), 'jsm')
    rc, response = req.send_request(
        app_route='/add_task_dag',
        message={'name': 'test dag', 'user': 'test user',
                 'dag_hash': 'test_{}'.format(random.randint(1, 1000)),
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    yield response['dag_id']


@pytest.fixture(scope='function')
def real_dag_id(real_jsm_jqs, db_cfg):
    import random
    from jobmon.client.the_client_config import get_the_client_config
    from jobmon.client.requester import Requester

    req = Requester(get_the_client_config(), 'jsm')
    rc, response = req.send_request(
        app_route='/add_task_dag',
        message={'name': 'test dag', 'user': 'test user',
                 'dag_hash': 'test_{}'.format(random.randint(1, 1000)),
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    yield response['dag_id']


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
def job_list_manager_sge(real_dag_id, tmpdir_factory):

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_jenkins')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=True,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def dag(db_cfg, no_requests_jsm_jqs, request):
    """Use a fixture for dag creation so that the dags' JobInstanceFactories
    and JobInstanceReconcilers get cleaned up after each test"""
    dag = TaskDag(name=request.node.name, interrupt_on_error=False)
    yield dag


@pytest.fixture(scope='function')
def real_dag(db_cfg, real_jsm_jqs, request):
    """Use a fixture for dag creation so that the dags' JobInstanceFactories
    and JobInstanceReconcilers get cleaned up after each test"""
    executor = SGEExecutor()
    dag = TaskDag(name=request.node.name, executor=executor,
                  interrupt_on_error=False)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


@pytest.fixture(scope='function')
def dag_factory(db_cfg, real_jsm_jqs, request):
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
