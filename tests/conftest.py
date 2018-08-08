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

from .server_test_config import config
from .client_test_config import config as client_config
from jobmon.server import database
from jobmon.server import database_loaders
from jobmon.client.executors.sge import SGEExecutor
from jobmon.client.worker_node.job_list_manager import JobListManager
from jobmon.client.workflow.task_dag import TaskDag
from jobmon.attributes import attribute_database_loaders


# NOTE: there are two types of tests that conftest sets up. 1. Using the real
# flask dev server, to allow us to do real testing of connections to the server
# --see real_jqs_jsm, real_dag_id, etc. 2. Using the fake server, called
# TestClient() which allows us to test certain parts of the code without
# spinning up a webserver--see no_requests_jsm_jqs. Any test that
# requires connection to more than one service (i.e JQS and JSM) requires that
# we use the real dev server.


@pytest.fixture(scope="session")
def teardown_edb(request):
    def teardown():
        database.Session.close_all()
        database.engine.dispose()
        config.session_edb.stop()
    request.addfinalizer(teardown)


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_server_config(monkeypatch):
    from jobmon.server import database

    def config_patch():
        from .server_test_config import config
        return config
    cfg = config_patch()
    monkeypatch.setattr(database, 'config', cfg)


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_client_config(monkeypatch):
    from jobmon.client import requester
    from jobmon.client.worker_node import job_factory, job_instance_factory, \
        job_instance_intercom, job_instance_reconciler, job_list_manager
    from jobmon.client.executors import base
    from jobmon.client.workflow import workflow, workflow_run, task_dag_factory
    from jobmon.server.services.health_monitor import health_monitor

    def config_patch():
        from .client_test_config import config
        return config
    cfg = config_patch()
    monkeypatch.setattr(requester, 'config', cfg)
    monkeypatch.setattr(job_factory, 'config', cfg)
    monkeypatch.setattr(job_instance_factory, 'config', cfg)
    monkeypatch.setattr(job_instance_intercom, 'config', cfg)
    monkeypatch.setattr(job_instance_reconciler, 'config', cfg)
    monkeypatch.setattr(job_list_manager, 'config', cfg)
    monkeypatch.setattr(base, 'config', cfg)
    monkeypatch.setattr(workflow, 'config', cfg)
    monkeypatch.setattr(workflow_run, 'config', cfg)
    monkeypatch.setattr(task_dag_factory, 'config', cfg)
    monkeypatch.setattr(health_monitor, 'config', cfg)


@pytest.fixture(scope='function')
def db_cfg():

    database_loaders.delete_job_db()
    database_loaders.create_job_db()
    try:
        with database.session_scope() as session:
            database_loaders.load_default_statuses(session)
            attribute_database_loaders.load_attribute_types(session)
    except IntegrityError:
        pass

    yield config


@pytest.fixture(scope='session')
def real_jsm_jqs():
    import multiprocessing as mp
    from tests.run_services import run_jsm, run_jqs

    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_jsm, args=(client_config.jsm_port,))
    p1.start()

    p2 = ctx.Process(target=run_jqs, args=(client_config.jqs_port,))
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

    req = Requester(client_config.jsm_port, host=socket.gethostname())
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
    from jobmon.client.requester import Requester

    req = Requester(client_config.jsm_port, host=socket.gethostname())
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
