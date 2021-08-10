import json

from jobmon.log_config import configure_logger
from jobmon.requester import Requester
from jobmon.client.api import Tool
import pytest


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(cluster_name="sequential",
                                                 compute_resources={"queue": "null.q"})
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


@pytest.fixture(scope="function")
def log_config(requester_in_memory, tmp_path):

    requester_in_memory.get("/")  # trigger logging setup
    filepath = str(tmp_path) + ".log"

    # override base config
    add_handler = {
        "default": {
            "class": "logging.FileHandler",
            "formatter": "json",
            "filename": filepath
        }
    }
    configure_logger("jobmon.server.web", add_handler)
    yield filepath
    configure_logger("jobmon.server.web")


@pytest.mark.skip()
def test_server_logging_format(requester_in_memory, log_config, tool, task_template):

    wf = tool.create_workflow("test_server")
    task_a = task_template.create_task(arg="echo r")
    wf.add_task(task_a)
    wf.bind()

    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            assert "blueprint" in log_dict.keys()


def test_add_structlog_context(requester_in_memory, log_config):
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester.send_request("/client/health", {}, "get", tenacious=False)
    requester.send_request("/client/health", {}, "post", tenacious=False)
    requester.send_request("/client/health", {}, "put", tenacious=False)
    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            for key in added_context.keys():
                assert key in log_dict.keys()
            for val in added_context.values():
                assert val in log_dict.values()


def test_error_handling(requester_in_memory, log_config, monkeypatch):
    from jobmon.server.web.routes.blueprints import client_routes

    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)
    monkeypatch.setattr(client_routes, "_get_time", raise_error)

    requester = Requester("")
    requester.send_request("/client/health", {}, "get", tenacious=False)
    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            if "exception" in log_dict.keys():
                assert msg in log_dict["exception"]
                assert "Traceback" in log_dict["exception"]
                captured_exception = True

    assert captured_exception


def test_server_500(requester_in_memory):
    test_requester = Requester('')
    rc, resp = test_requester._send_request(
        app_route='/client/test_bad',
        message={},
        request_type='get'
    )
    assert rc == 500
    assert 'MySQLdb._exceptions.ProgrammingError' in resp['error']['exception_message']
