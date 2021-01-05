import pytest

import structlog


@pytest.fixture(scope='module')
def log_setup():
    from jobmon.log_config import configure_logger
    add_handler = {
        "default": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "json",
        }
    }
    configure_logger("jobmon.client", add_handler)
    yield
    configure_logger("jobmon.client")


def test_client_structlogs(log_setup, client_env, capsys):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    workflow = UnknownWorkflow("test_client_logging", executor_class="SequentialExecutor")

    task_a = BashTask("echo b", executor_class="SequentialExecutor")
    workflow.add_task(task_a)
    workflow.run()

    captured = capsys.readouterr()
    logs = captured.err.split('\n')
    for log in logs:
        if log:
            assert 'jobmon_version' in log
            assert 'logger' in log
            assert 'level' in log
            assert 'timestamp' in log


def test_requester_logging_injection(log_setup, client_env, capsys):
    from jobmon.requester import Requester

    logger = structlog.getLogger("jobmon.client.templates.bash_task")
    requester = Requester(client_env)
    requester.send_request("/time", {}, "get", logger, tenacious=False)
    captured = capsys.readouterr()
    logs = captured.err.split('\n')
    for log in logs:
        if log:
            assert 'jobmon.client.templates.bash_task' in log
