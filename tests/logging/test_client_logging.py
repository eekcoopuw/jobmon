import logging
import pytest
import structlog
from structlog.testing import LogCapture

logger = logging.getLogger(__name__)


def test_client_structlogs(db_cfg, client_env, capsys):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    workflow = UnknownWorkflow("test_client_logging",
                               executor_class="SequentialExecutor")

    task_a = BashTask(
        "echo b", executor_class="SequentialExecutor"
    )
    workflow.add_task(task_a)
    workflow.run()

    captured = capsys.readouterr()
    logs =captured.err.split('\n')
    for log in logs:
        if 'jobmon_version' not in log:
            assert "Starting" in log # Starting HTTP connection doesn't have full log setup
            break
        assert 'jobmon_version' in log
        assert 'logger' in log
        assert 'level' in log
        assert 'timestamp' in log
        # no 'blueprint' logs are in this output
