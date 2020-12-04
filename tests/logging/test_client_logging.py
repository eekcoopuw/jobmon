import logging

logger = logging.getLogger(__name__)


def test_client_structlogs(db_cfg, client_env, capsys):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.log_config import configure_logger

    add_handler = {
        "default": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "json",
        }
    }
    configure_logger("jobmon.client", add_handler)

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
