from builtins import str

import os
import pytest
import pwd
import shutil
import uuid
import socket
import logging
from datetime import datetime
from time import sleep
from sqlalchemy.exc import IntegrityError
from cluster_utils.ephemerdb import create_ephemerdb


logger = logging.getLogger(__name__)


@pytest.fixture(scope='session', autouse=True)
def ephemera_conn_str():
    edb = create_ephemerdb()
    conn_str = edb.start()

    os.environ['conn_str'] = conn_str
    yield conn_str

    # from jobmon.server import database
    # database.Session.close_all()
    # if database.engine:
    #     database.engine.dispose()
    # edb.stop()


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


@pytest.fixture(autouse=True)
def env_var(monkeypatch, ephemera_conn_str):
    monkeypatch.setenv("host", socket.gethostname())
    monkeypatch.setenv("conn_str", ephemera_conn_str)


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
        logger.info("got integrity error")
        pass


@pytest.fixture(scope='session')
def real_jsm_jqs():
    import multiprocessing as mp
    from tests.run_services import run_jsm, run_jqs

    os.environ['host'] = socket.gethostname()

    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_jsm, args=())
    p1.start()

    p2 = ctx.Process(target=run_jqs, args=())
    p2.start()

    sleep(30)
    yield

    p1.terminate()
    p2.terminate()


@pytest.fixture(scope='session')
def jsm_jqs():
    from jobmon.server.services.job_state_manager.app \
        import create_app as jsm_get_app
    from jobmon.server.services.job_query_server.app import \
        create_app as jqs_get_app

    os.environ['host'] = socket.gethostname()

    jsm_app = jsm_get_app(host=os.environ['host'],
                          conn_str=os.environ['conn_str'])
    jqs_app = jqs_get_app(host=os.environ['host'],
                          conn_str=os.environ['conn_str'])
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
        app_route='/task_dag',
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
        app_route='/task_dag',
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
    jlm.disconnect()


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
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


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
