from builtins import str
from datetime import datetime
import glob
import logging
import os
import pwd
import re
import shutil
import socket
import sys
import uuid
from time import sleep

import pytest
import requests

from cluster_utils.ephemerdb import create_ephemerdb

from jobmon.client import BashTask, Workflow

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session', autouse=True)
def ephemera():
    """Note: this function must be placed before the other imports
    because the ephemera db has to be started before any other code
    imports the_server_config
    """
    edb = create_ephemerdb(elevated_privileges=True)
    edb.db_name = "docker"
    conn_str = edb.start()
    # use the ephemera db root privileges (root: singularity_root) otherwise
    # you will not see changes to the database
    logger.info(f"Database connection {conn_str}")
    print(f"****** Database connection {conn_str}")

    # load schema
    here = os.path.dirname(__file__)
    schema_dir = os.path.join(here, "..",
                              "jobmon/server/deployment/container/db")
    schema_files = glob.glob(os.path.join(schema_dir, "*.sql"))
    for file in sorted(schema_files):
        edb.execute_sql_script(file)

    # get connection info
    pattern = ("mysql://(?P<user>.*):(?P<pass>.*)"
               "@(?P<host>.*):(?P<port>.*)/(?P<db>.*)")
    result = re.search(pattern, conn_str)
    db_conn_dict = result.groupdict()
    cfg = {
        "DB_HOST": db_conn_dict["host"],
        "DB_PORT": db_conn_dict["port"],
        "DB_USER": db_conn_dict["user"],
        "DB_PASS": db_conn_dict["pass"],
        "DB_NAME": db_conn_dict["db"]
    }

    yield cfg


@pytest.fixture(scope='session')
def real_jsm_jqs(ephemera):
    """This starts the flask dev server in separate processes"""
    import multiprocessing as mp
    from tests.run_services import run_web_service

    # spawn ensures that no attributes are copied to the new process. Python
    # starts from scratch
    jobmon_host = socket.gethostname()
    jobmon_port = str(10_000 + os.getpid() % 30_000)
    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_web_service, args=(
        jobmon_port,
        ephemera["DB_HOST"],
        ephemera["DB_PORT"],
        ephemera["DB_USER"],
        ephemera["DB_PASS"],
        ephemera["DB_NAME"],
    ))
    p1.start()

    # Wait for it to be up
    status = 404
    count = 0
    max_tries = 60
    while not status == 200 and count < max_tries:
        try:
            count += 1
            r = requests.get(f'http://{jobmon_host}:{jobmon_port}')
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

    # These are tests, so set log level to  DEBUG
    message = {}
    requests.post((f'http://{jobmon_host}:{jobmon_port}/log_level/DEBUG'),
                  json=message,
                  headers={'Content-Type': 'application/json'})

    yield {"JOBMON_HOST": jobmon_host, "JOBMON_PORT": jobmon_port}

    p1.terminate()


@pytest.fixture(scope='function')
def db_cfg(ephemera):
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh"""
    from jobmon.models import database_loaders
    from jobmon.models import DB
    from jobmon.server import create_app
    from jobmon.server.config import ServerConfig

    # The create_app call sets up database connections
    server_config = ServerConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        wf_slack_channel=None,
        node_slack_channel=None,
        slack_token=None)
    app = create_app(server_config)

    yield {'app': app, 'DB': DB}

    with app.app_context():
        database_loaders.clean_job_db(DB)


@pytest.fixture(scope='function')
def env_var(real_jsm_jqs, monkeypatch):
    from jobmon.client import shared_requester, client_config
    from jobmon.client.client_config import ClientConfig

    cc = ClientConfig.from_defaults()
    cc.host = real_jsm_jqs["JOBMON_HOST"]
    cc.port = real_jsm_jqs["JOBMON_PORT"]

    monkeypatch.setattr(shared_requester, "url", cc.url)
    monkeypatch.setattr(client_config, 'heartbeat_interval', 10)
    monkeypatch.setattr(client_config, 'report_by_buffer', 2.1)
    monkeypatch.setattr(client_config, 'reconciliation_interval', 5)

    monkeypatch.setenv("JOBMON_SERVER_SQDN", real_jsm_jqs["JOBMON_HOST"])
    monkeypatch.setenv("JOBMON_SERVICE_PORT", real_jsm_jqs["JOBMON_PORT"])


@pytest.fixture(scope='function')
def real_dag_id(env_var):
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
def jlm_sge_daemon(real_dag_id, tmpdir_factory):
    """This creates a job_list_manager that uses the SGEExecutor, does
    start the JobInstanceFactory and JobReconciler threads
    It has short reconciliation intervals so that the
    tests run faster than in production.
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_tools')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=True,
                         job_instantiation_interval=1)
    yield jlm
    jlm.disconnect()


@pytest.fixture
def jlm_sge_no_daemon(real_dag_id, tmpdir_factory):
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_tools')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def real_dag(db_cfg, env_var, request):
    """"This is a fixture for dag creation that uses the real Flask dev server
    so that the dags' JobInstanceFactory and JobInstanceReconcilers get
    cleaned up after each test
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.workflow.task_dag import TaskDag
    # The workflow creates the executor, not the workflow.
    # Hence we must create one here and pass it in
    executor = SGEExecutor(project='proj_tools')
    dag = TaskDag(name=request.node.name, executor=executor)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


@pytest.fixture(scope='function')
def dag_factory(db_cfg, env_var, request):
    """This is a fixture for creation of lots dag creation that uses the real
    Flask dev server, so that the dags' JobInstanceFactory and
    JobInstanceReconcilers get cleaned up after each test
    """
    from jobmon.client.swarm.workflow.task_dag import TaskDag
    dags = []

    def factory(executor):
        dag = TaskDag(name=request.node.name, executor=executor)
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
                      'remote_sleep_and_write.py', 'kill.py', 'exceed_mem.py']
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


@pytest.fixture
def simple_workflow(env_var, db_cfg):
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow

    t1 = BashTask("sleep 1", num_cores=1, m_mem_free='1G')
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1, m_mem_free='1G')
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1, m_mem_free='1G')

    wfa = "my_simple_dag"
    workflow = Workflow(wfa)
    workflow.add_tasks([t1, t2, t3])
    workflow.execute()
    return workflow


@pytest.fixture
def simple_workflow_w_errors(env_var, db_cfg):
    # Used in test_workflow_[ab]
    t1 = BashTask("sleep 1", num_cores=1, m_mem_free='2G', queue="all.q",
                  j_resource=False)
    t2 = BashTask("not_a_command 1", upstream_tasks=[t1], num_cores=1,
                  m_mem_free='2G', queue="all.q", j_resource=False)
    t3 = BashTask("sleep 30", upstream_tasks=[t1], max_runtime_seconds=4,
                  num_cores=1, m_mem_free='2G', queue="all.q", j_resource=False)
    t4 = BashTask("not_a_command 3", upstream_tasks=[t2, t3], num_cores=1,
                  m_mem_free='2G', queue="all.q", j_resource=False)

    workflow = Workflow("my_failing_args", project='ihme_general')
    workflow.add_tasks([t1, t2, t3, t4])
    workflow.execute()
    return workflow
