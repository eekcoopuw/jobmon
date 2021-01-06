import glob
import json
import logging
import os
import platform
import pwd
import re
import shutil
import socket
import sys
from time import sleep
import uuid

from filelock import FileLock
import pytest
import requests
from sqlalchemy import create_engine

from cluster_utils.ephemerdb import create_ephemerdb, MARIADB


logger = logging.getLogger(__name__)


@pytest.fixture(scope='session', autouse=True)
def set_mac_to_fork():
    """necessary for running tests on a mac with python 3.8 see:
    https://github.com/pytest-dev/pytest-flask/issues/104"""
    if platform.system() == 'Darwin':
        import multiprocessing
        multiprocessing.set_start_method("fork")


def boot_db() -> dict:
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
    create_dir = os.path.join(here, "..", "deployment/config/db")
    updates_dir = os.path.join(here, "..", "deployment/config/db/upgrade")

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
    return cfg


@pytest.fixture(scope="session")
def ephemera(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return boot_db()

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            data = json.loads(fn.read_text())
        else:
            data = boot_db()
            fn.write_text(json.dumps(data))
    return data


@pytest.fixture(scope='session')
def web_server_process(ephemera):
    """This starts the flask dev server in separate processes"""
    import multiprocessing as mp
    import signal

    # host info for connecting to web service
    # spawn ensures that no attributes are copied to the new process. Python
    # starts from scratch
    # The Jobmon server and ephemera can communicate via localhost,
    # But the worker node needs the FQDN of the jobmon server
    if sys.platform == "darwin":
        web_host = ephemera["DB_HOST"]
    else:
        web_host = socket.getfqdn()
    web_port = str(10_000 + os.getpid() % 30_000)

    # jobmon_cli string
    argstr = (
        'web_service test '
        f'--db_host {ephemera["DB_HOST"]} '
        f'--db_port {ephemera["DB_PORT"]} '
        f'--db_user {ephemera["DB_USER"]} '
        f'--db_pass {ephemera["DB_PASS"]} '
        f'--db_name {ephemera["DB_NAME"]} '
        f'--web_service_port {web_port}')

    def run_server_with_handler(argstr):
        def sigterm_handler(_signo, _stack_frame):
            # catch SIGTERM and shut down with 0 so pycov finalizers are run
            # Raises SystemExit(0):
            import sys
            sys.exit(0)
        from jobmon.server.cli import main

        signal.signal(signal.SIGTERM, sigterm_handler)
        main(argstr)

    ctx = mp.get_context('fork')
    p1 = ctx.Process(target=run_server_with_handler, args=(argstr,))
    p1.start()

    # Wait for it to be up
    status = 404
    count = 0
    max_tries = 60
    while not status == 200 and count < max_tries:
        try:
            count += 1
            r = requests.get(f'http://{web_host}:{web_port}/health')
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

    yield {"JOBMON_HOST": web_host, "JOBMON_PORT": web_port}

    # interrupt and join for coverage
    p1.terminate()
    p1.join()


@pytest.fixture(scope='session')
def db_cfg(ephemera):
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh"""
    from jobmon.server.web.models import DB
    from jobmon.server.web.api import WebConfig, create_app

    # The create_app call sets up database connections
    web_config = WebConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"])
    app = create_app(web_config)

    yield {'app': app, 'DB': DB, "server_config": web_config}


@pytest.fixture(scope='function')
def client_env(web_server_process, monkeypatch):
    from jobmon.client.client_config import ClientConfig
    monkeypatch.setenv("WEB_SERVICE_FQDN", web_server_process["JOBMON_HOST"])
    monkeypatch.setenv("WEB_SERVICE_PORT", web_server_process["JOBMON_PORT"])

    cc = ClientConfig(web_server_process["JOBMON_HOST"],  web_server_process["JOBMON_PORT"])
    yield cc.url


@pytest.fixture(scope='function')
def requester_no_retry(client_env):
    from jobmon.requester import Requester
    return Requester(client_env, max_retries=0)


@pytest.fixture(scope='module')
def tmp_out_dir():
    """This creates a new tmp_out_dir for every module"""
    user = pwd.getpwuid(os.getuid()).pw_name
    output_root = f'/ihme/scratch/users/{user}/tests/jobmon/{uuid.uuid4()}'
    yield output_root
    shutil.rmtree(output_root, ignore_errors=True)
