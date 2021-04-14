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


def _get_task():
    from tiny_structured_logger.lib.constants import LoggerConstants
    from tiny_structured_logger.lib import structured_logger
    logger = structured_logger.logger(
            log_tag=LoggerConstants.LogTags.CONSOLE_TAG,
            level=LoggerConstants.LogLevels.DEBUG.value,
            team="whatever",
            component="whatever",
            version="whatever",
            syslog_endpoint=None,
        )
    logger.info(
        msg="A fake log message for testing",
        bindings=dict(
            task_type="whatever",
            acause="whatever",
            name="whatever",
            executor_class="whatever",
            script="whatever",
        ),
    )
    from jobmon.client.api import BashTask
    task = BashTask("echo 1", executor_class="SequentialExecutor")
    return task

def test_tiny_structured_logger(client_env, capsys):
    """This is to make sure our jobmon clien works with tiny_structured_logger,
       which is widely used by FHS.
       The code is a simplified version of fhs-pipeline-mortality-orchestration/tests/test_stage_3.py,
       that only keeps the part that causes GBDSCI-3468.
    """
    from tiny_structured_logger.lib.constants import LoggerConstants
    from jobmon.client.api import Tool, UnknownWorkflow
    import logging
    # add a root logger handler
    logging.getLogger().addHandler(logging.StreamHandler())
    wf = UnknownWorkflow("test_tiny_structured_logger",
                          executor_class="SequentialExecutor")
    task = _get_task()
    wf.add_task(task)
    captured = capsys.readouterr()
    logs = captured.err.split('\n')
    for log in logs:
        if log:
            assert 'KeyError' not in log