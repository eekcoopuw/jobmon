import pytest

import structlog







def test_tiny_structured_logger(client_env, capsys):
    """This is to make sure our jobmon clien works with tiny_structured_logger,
       which is widely used by FHS.
       The code is a simplified version of fhs-pipeline-mortality-orchestration/tests/test_stage_3.py,
       that only keeps the part that causes GBDSCI-3468.
    """
    from tiny_structured_logger.lib.constants import LoggerConstants
    from jobmon.client.api import Tool, UnknownWorkflow
    from tiny_structured_logger.lib import structured_logger
    from jobmon.client.client_logging import ClientLogging
    import logging
    def _get_task():
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
    #add a root logger handler
    ClientLogging().attach(handler=logging.StreamHandler())
    wf = UnknownWorkflow("test_tiny_structured_logger",
                          executor_class="SequentialExecutor")
    task = _get_task()
    wf.add_task(task)
    captured = capsys.readouterr()
    logs = captured.err.split('\n')
    for log in logs:
        if log:
            assert 'KeyError' not in log