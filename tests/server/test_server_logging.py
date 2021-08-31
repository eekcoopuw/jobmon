import json

from jobmon.server.web.log_config import configure_logger
from jobmon.requester import Requester
import pytest


@pytest.fixture(scope="function")
def log_config(web_server_in_memory, tmp_path):
    web_server_in_memory.get("/")  # trigger logging setup
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


def test_add_structlog_context(requester_in_memory, log_config):
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester.send_request("/health", {}, "get", tenacious=False)
    requester.send_request("/health", {}, "post", tenacious=False)
    requester.send_request("/health", {}, "put", tenacious=False)
    with open(log_config, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            for key in added_context.keys():
                assert key in log_dict.keys()
            for val in added_context.values():
                assert val in log_dict.values()


def test_error_handling(requester_in_memory, log_config, monkeypatch):
    from jobmon.server.web import routes

    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)
    monkeypatch.setattr(routes, "_get_time", raise_error)

    requester = Requester("")
    requester.send_request("/health", {}, "get", tenacious=False)
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
        app_route='/test_bad',
        message={},
        request_type='get'
    )
    assert rc == 500
    assert 'MySQLdb._exceptions.ProgrammingError' in resp['error']['exception_message']
