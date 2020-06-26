from builtins import str
import glob
import logging
import os
import pwd
import re
import shutil
import socket
import uuid
from time import sleep

import pytest
import requests

from cluster_utils.ephemerdb import create_ephemerdb, MARIADB
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


@pytest.fixture(scope='global')
def ephemera():
    """Note: this function must be placed before the other imports
    because the ephemera db has to be started before any other code
    imports the_server_config
    """
    edb = create_ephemerdb(elevated_privileges=True, database_type=MARIADB)
    edb.db_name = "docker"
    conn_str = edb.start()

    # Set the time zone
    eng = create_engine(edb.root_conn_str)
    with eng.connect() as conn:
        conn.execute("SET GLOBAL time_zone = 'America/Los_Angeles'")

    # use the ephemera db root privileges (root: singularity_root) otherwise
    # you will not see changes to the database
    logger.info(f"Database connection {conn_str}")
    print(f"****** Database connection {conn_str}")

    # load schema
    here = os.path.dirname(__file__)
    create_dir = os.path.join(here, "..",
                              "jobmon/server/deployment/container/db")
    create_files = glob.glob(os.path.join(create_dir, "*.sql"))
    for file in sorted(create_files):
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


@pytest.fixture(scope='session')
def db_cfg(ephemera):
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh"""
    from jobmon.models import DB
    from jobmon.server import create_app
    from jobmon.server.server_config import ServerConfig

    # The create_app call sets up database connections
    server_config = ServerConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        slack_token=None,
        wf_slack_channel=None,
        node_slack_channel=None)
    app = create_app(server_config)

    yield {'app': app, 'DB': DB, "server_config": server_config}


@pytest.fixture(scope='function')
def client_env(real_jsm_jqs, monkeypatch):
    from jobmon.client import shared_requester
    from jobmon.client.requests.connection_config import ConnectionConfig
    from jobmon import config

    monkeypatch.setenv("JOBMON_SERVER_SQDN", real_jsm_jqs["JOBMON_HOST"])
    monkeypatch.setenv("JOBMON_SERVICE_PORT", real_jsm_jqs["JOBMON_PORT"])
    config.jobmon_server_sqdn = real_jsm_jqs["JOBMON_HOST"]
    config.jobmon_service_port = real_jsm_jqs["JOBMON_PORT"]
    cc = ConnectionConfig(real_jsm_jqs["JOBMON_HOST"],
                          real_jsm_jqs["JOBMON_PORT"])

    # modify shared requester
    monkeypatch.setattr(shared_requester, "url", cc.url)


@pytest.fixture(scope='module')
def tmp_out_dir():
    """This creates a new tmp_out_dir for every module"""
    user = pwd.getpwuid(os.getuid()).pw_name
    output_root = f'/ihme/scratch/users/{user}/tests/jobmon/{uuid.uuid4()}'
    yield output_root
    shutil.rmtree(output_root, ignore_errors=True)


# # @pytest.fixture(autouse=True)
# # def execution_test_script_perms():
# #     executed_files = ['executor_args_check.py', 'simple_R_script.r',
# #                       'simple_stata_script.do', 'memory_usage_array.py',
# #                       'remote_sleep_and_write.py', 'kill.py', 'exceed_mem.py']
# #     if sys.version_info.major == 3:
# #         perms = int("0o755", 8)
# #     else:
# #         perms = int("0755", 8)
# #     path = os.path.dirname(os.path.realpath(__file__))
# #     shell_path = os.path.join(path, 'shellfiles/')
# #     files = os.listdir(shell_path)
# #     os.chmod(shell_path, perms)
# #     for file in files:
# #         try:
# #             os.chmod(f'{shell_path}{file}', perms)
# #         except Exception as e:
# #             raise e
# #     for file in executed_files:
# #         try:
# #             os.chmod(f'{path}/{file}', perms)
# #         except Exception as e:
# #             raise e

