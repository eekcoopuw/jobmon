import json
import logging
import os
import platform

import pytest

from jobmon.test_utils import test_server_config, WebServerProcess, ephemera_db_instance

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def set_mac_to_fork():
    """necessary for running tests on a mac with python 3.8 see:
    https://github.com/pytest-dev/pytest-flask/issues/104"""
    if platform.system() == "Darwin":
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


@pytest.fixture(scope="session")
def web_server_process(ephemera):
    """This starts the flask dev server in separate processes"""
    with WebServerProcess(ephemera) as web:
        yield {"JOBMON_HOST": web.web_host, "JOBMON_PORT": web.web_port}


@pytest.fixture(scope="session")
def db_cfg(ephemera) -> dict:
    return test_server_config(ephemera)


@pytest.fixture(scope="function")
def client_env(web_server_process, monkeypatch):

    monkeypatch.setenv("WEB_SERVICE_FQDN", web_server_process["JOBMON_HOST"])
    monkeypatch.setenv("WEB_SERVICE_PORT", web_server_process["JOBMON_PORT"])

    from jobmon.client.client_config import ClientConfig

    cc = ClientConfig(
        web_server_process["JOBMON_HOST"], web_server_process["JOBMON_PORT"], 30, 3.1
    )
    yield cc.url


@pytest.fixture(scope="function")
def requester_no_retry(client_env):
    from jobmon.requester import Requester

    return Requester(client_env, max_retries=0)


@pytest.fixture(scope="session")
def web_server_in_memory(ephemera):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server.web.start import create_app
    from jobmon.server.web.web_config import WebConfig

    # The create_app call sets up database connections
    server_config = WebConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
    )
    app = create_app(server_config)
    app.config["TESTING"] = True
    client = app.test_client()
    yield client


@pytest.fixture(scope='module')
def usage_integrator_config(ephemera):
    """This creates a usage integrator config.

    Only created if a test with the usage_integrator marker is selected."""

    from jobmon.server.usage_integration.config import UsageConfig

    json_path = os.path.join(os.path.dirname(__file__),
                             'usage_integration/integrator_secrets.json')

    # Read in the local integrator_secrets.json file, or create it if it doesn't exist
    try:
        with open(json_path, 'r') as f:
            integrator_config_dict = json.loads(f.read())
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        # Missing or improperly formatted json. Pull from environment variables. If not found
        # default to None, since this fixture shouldn't be used anyways.
        integrator_config_dict = {
            "DB_HOST_SLURM_SDB": os.getenv("DB_HOST_SLURM_SDB"),
            "DB_PASS_SLURM_SDB": os.getenv("DB_PASS_SLURM_SDB"),
            "DB_USER_SLURM_SDB": os.getenv("DB_USER_SLURM_SDB"),
            "DB_NAME_SLURM_SDB": os.getenv("DB_NAME_SLURM_SDB"),
            "DB_PORT_SLURM_SDB": os.getenv("DB_PORT_SLURM_SDB")
        }

    # Combine with ephemera to create the usage integrator config
    integrator_config = UsageConfig(
        db_host=ephemera["DB_HOST"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        db_port=ephemera["DB_PORT"],
        db_host_slurm_sdb=integrator_config_dict["DB_HOST_SLURM_SDB"],
        db_user_slurm_sdb=integrator_config_dict["DB_USER_SLURM_SDB"],
        db_pass_slurm_sdb=integrator_config_dict["DB_PASS_SLURM_SDB"],
        db_name_slurm_sdb=integrator_config_dict["DB_NAME_SLURM_SDB"],
        db_port_slurm_sdb=integrator_config_dict["DB_PORT_SLURM_SDB"],
        slurm_polling_interval=10,
        slurm_max_update_per_second=100,
        slurm_cluster='slurm'
    )
    return integrator_config


@pytest.fixture(scope="module")
def usage_integrator(ephemera, usage_integrator_config):
    """Creates a configured instance of the usage integrator."""

    from jobmon.server.usage_integration.usage_integrator import UsageIntegrator

    # Create the usage integrator, and yield. On teardown close the connections
    integrator = UsageIntegrator(usage_integrator_config)
    yield integrator

    integrator.session.close()
    integrator.session_slurm_sdb.close()


def get_test_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if "application/json" in response.headers.get("Content-Type"):
        content = response.json
    elif "text/html" in response.headers.get("Content-Type"):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope="function")
def requester_in_memory(monkeypatch, web_server_in_memory):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon import requester

    monkeypatch.setenv("WEB_SERVICE_FQDN", "1")
    monkeypatch.setenv("WEB_SERVICE_PORT", "2")

    def get_in_mem(url, params, data, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return web_server_in_memory.get(
            path=url, query_string=params, data=data, headers=headers
        )

    def post_in_mem(url, json, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return web_server_in_memory.post(url, json=json, headers=headers)

    def put_in_mem(url, json, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return web_server_in_memory.put(url, json=json, headers=headers)

    monkeypatch.setattr(requests, "get", get_in_mem)
    monkeypatch.setattr(requests, "post", post_in_mem)
    monkeypatch.setattr(requests, "put", post_in_mem)
    monkeypatch.setattr(requester, "get_content", get_test_content)


def get_task_template(tool, template_name):
    tool.get_task_template(
        template_name=template_name,
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )


@pytest.fixture
def tool(db_cfg, client_env):
    from jobmon.client.api import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tool.get_task_template(
        template_name="array_template",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    get_task_template(tool, "phase_1")
    get_task_template(tool, "phase_2")
    get_task_template(tool, "phase_3")
    return tool


@pytest.fixture
def task_template(tool):
    return tool.active_task_templates["simple_template"]


@pytest.fixture
def array_template(tool):
    return tool.active_task_templates["array_template"]
