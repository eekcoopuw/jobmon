import logging
import platform

import pytest

from jobmon.test_utils import test_server_config, WebServerProcess, ephemera_db_instance


logger = logging.getLogger(__name__)


@pytest.fixture(scope='session', autouse=True)
def set_mac_to_fork():
    """necessary for running tests on a mac with python 3.8 see:
    https://github.com/pytest-dev/pytest-flask/issues/104"""
    if platform.system() == 'Darwin':
        import multiprocessing
        multiprocessing.set_start_method("fork")


@pytest.fixture(scope="session")
def ephemera(tmp_path_factory, worker_id) -> dict:
    """
    Boots exactly one instance of the test ephemera database

    Returns:
      a dictionary with connection parameters
    """
    return ephemera_db_instance(tmp_path_factory, worker_id)


@pytest.fixture(scope='session')
def web_server_process(ephemera):
    """This starts the flask dev server in separate processes"""
    with WebServerProcess(ephemera) as web:
        yield {"JOBMON_HOST": web.web_host, "JOBMON_PORT": web.web_port}


@pytest.fixture(scope='session')
def db_cfg(ephemera) -> dict:
    return test_server_config(ephemera)


@pytest.fixture(scope='function')
def client_env(web_server_process, monkeypatch):
    from jobmon.client.client_config import ClientConfig
    monkeypatch.setenv("WEB_SERVICE_FQDN", web_server_process["JOBMON_HOST"])
    monkeypatch.setenv("WEB_SERVICE_PORT", web_server_process["JOBMON_PORT"])

    cc = ClientConfig(web_server_process["JOBMON_HOST"], web_server_process["JOBMON_PORT"],
                      30, 3.1)
    yield cc.url


@pytest.fixture(scope='function')
def requester_no_retry(client_env):
    from jobmon.requester import Requester
    return Requester(client_env, max_retries=0)
