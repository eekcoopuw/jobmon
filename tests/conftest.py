import glob
import json
import logging
import os
import pwd
import re
import shutil
import socket
from time import sleep
import uuid

from filelock import FileLock
import pytest
import requests
from sqlalchemy import create_engine

from cluster_utils.ephemerdb import create_ephemerdb, MARIADB

from jobmon.server.cli import main


logger = logging.getLogger(__name__)


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
    create_dir = os.path.join(here, "..", "jobmon/server/deployment/container/db")
    updates_dir = os.path.join(here, "..", "jobmon/server/deployment/container/db/upgrade")

    create_files = glob.glob(os.path.join(create_dir, "*.sql"))
    create_files.extend(glob.glob(os.path.join(updates_dir, "upgrade002-Guppyette.sql")))

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
def real_jsm_jqs(ephemera):
    """This starts the flask dev server in separate processes"""
    import multiprocessing as mp
    import signal

    # host info for connecting to web service
    # spawn ensures that no attributes are copied to the new process. Python
    # starts from scratch
    # The Jobmon server and ephemera can communicate via localhost,
    # But the worker node needs the FQDN of the jobmon server
    jobmon_host = socket.getfqdn()
    jobmon_port = str(10_000 + os.getpid() % 30_000)

    # cli string
    argstr = (
        'web_service test '
        f'--db_host {ephemera["DB_HOST"]} '
        f'--db_port {ephemera["DB_PORT"]} '
        f'--db_user {ephemera["DB_USER"]} '
        f'--db_pass {ephemera["DB_PASS"]} '
        f'--db_name {ephemera["DB_NAME"]} '
        f'--web_service_port {jobmon_port}')

    def run_server_with_handler(argstr):
        def sigterm_handler(_signo, _stack_frame):
            # catch SIGTERM and shut down with 0 so pycov finalizers are run
            # Raises SystemExit(0):
            import sys
            sys.exit(0)
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
            r = requests.get(f'http://{jobmon_host}:{jobmon_port}/health')
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

    yield {"JOBMON_HOST": jobmon_host, "JOBMON_PORT": jobmon_port}

    # interrupt and join for coverage
    p1.terminate()


@pytest.fixture(scope='session')
def db_cfg(ephemera):
    """This run at the beginning of every function to tear down the db
    of the previous test and restart it fresh"""
    from jobmon.models import DB
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
def client_env(real_jsm_jqs, monkeypatch):
    from jobmon.client.client_config import ClientConfig
    monkeypatch.setenv("WEB_SERVICE_FQDN", real_jsm_jqs["JOBMON_HOST"])
    monkeypatch.setenv("WEB_SERVICE_PORT", real_jsm_jqs["JOBMON_PORT"])

    cc = ClientConfig(real_jsm_jqs["JOBMON_HOST"],  real_jsm_jqs["JOBMON_PORT"])
    yield cc.url


@pytest.fixture(scope='module')
def tmp_out_dir():
    """This creates a new tmp_out_dir for every module"""
    user = pwd.getpwuid(os.getuid()).pw_name
    output_root = f'/ihme/scratch/users/{user}/tests/jobmon/{uuid.uuid4()}'
    yield output_root
    shutil.rmtree(output_root, ignore_errors=True)
