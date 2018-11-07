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
    """Note: this function must be placed before the other imports
    because the ephemera db has to be started before any other code
    imports the_server_config
    """
    edb = create_ephemerdb()
    conn_str = edb.start()



    os.environ['CONN_STR'] = conn_str
    yield conn_str


from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.task_dag import TaskDag
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
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
    """These two env variables are what tell theconfigs that we're running tests,
    not production code
    """
    monkeypatch.setenv("RUN_HOST", socket.gethostname())
    monkeypatch.setenv("CONN_STR", ephemera_conn_str)


@pytest.fixture(scope='function')
def db_cfg():
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh
    """
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
    """This starts the flask dev server in separate processes"""
    import multiprocessing as mp
    from tests.run_services import run_jsm, run_jqs

    os.environ['RUN_HOST'] = socket.gethostname()

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
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server.services.job_state_manager.app \
        import create_app as jsm_get_app
    from jobmon.server.services.job_query_server.app import \
        create_app as jqs_get_app

    os.environ['RUN_HOST'] = socket.gethostname()

    jsm_app = jsm_get_app(host=os.environ['RUN_HOST'],
                          conn_str=os.environ['CONN_STR'])
    jqs_app = jqs_get_app(host=os.environ['RUN_HOST'],
                          conn_str=os.environ['CONN_STR'])
    jsm_app.config['TESTING'] = True
    jqs_app.config['TESTING'] = True
    jsm_client = jsm_app.test_client()
    jqs_client = jqs_app.test_client()
    yield jsm_client, jqs_client


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
    return content


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
    """This uses the real Flask dev server to create a dag in the db and
    return the dag_id
    """
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
    jlm = JobListManager(dag_id, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge(real_dag_id, tmpdir_factory):
    """This creates a job_list_manager that uses the SGEExecutor, does
    start the JobInstanceFactory and JobReconciler threads, and does not
    interrupt on error
    """
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
    """This is a fixture for dag creation that uses the test_client,
    so that the dags' JobInstanceFactory and JobInstanceReconcilers get
    cleaned up after each test
    """
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
