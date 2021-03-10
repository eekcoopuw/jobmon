import json

from jobmon.log_config import configure_logger
from jobmon.requester import Requester

import pytest


@pytest.fixture(scope="function")
def log_config(test_app, tmp_path):

    test_app.get("/")  # trigger logging setup
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


def test_server_logging_format(web_server_in_memory, log_config):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    wf = UnknownWorkflow("test_server")
    task_a = BashTask("echo r", executor_class="SequentialExecutor")
    wf.add_task(task_a)
    wf.bind()
    wf.requester.send_request(
        app_route='/client/workflow',
        message={
            "tool_version_id": wf.tool_version_id,
            "dag_id": 'blah',
            "workflow_args_hash": wf.workflow_args_hash,
            "task_hash": wf.task_hash
        },
        request_type='post'
    )

    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            assert "blueprint" in log_dict.keys()


def test_add_structlog_context(web_server_in_memory, log_config):
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


def test_error_handling(web_server_in_memory, log_config, monkeypatch):
    from jobmon.server.web.routes import jobmon_client_routes

    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)
    monkeypatch.setattr(jobmon_client_routes, "_get_time", raise_error)

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


def test_server_500(web_server_in_memory):
    test_requester = Requester('')
    rc, resp = test_requester._send_request(
        app_route='/client/test_bad',
        message={},
        request_type='get'
    )
    assert rc == 500
    assert 'MySQLdb._exceptions.ProgrammingError' in resp['error']['exception_message']
