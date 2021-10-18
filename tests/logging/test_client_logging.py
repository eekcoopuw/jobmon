import pytest

import logging
from datetime import date

def test_client_logging_default_format(client_env, capsys):
    from jobmon.client.client_logging import ClientLogging
    ClientLogging().attach(logger_name="test.test")
    logger = logging.getLogger("test.test")
    logger.info("This is a test")
    captured = capsys.readouterr()
    logs = captured.out.split('\n')
    # should only contain two lines, one empty, one above log
    for log in logs:
        if log:
            # check format and message
            assert date.today().strftime("%Y-%m-%d") in log
            assert "test.test" in log
            assert "INFO" in log
            assert "This is a test" in log

def test_client_logging_customized_handler(client_env, capsys):
    from jobmon.client.client_logging import ClientLogging
    h = logging.StreamHandler() #stderr
    # This formatter logs nothing but the fixed msg.
    # Nobody would create a log like this; thus, it proves it's using my logger.
    h.setFormatter("I log this instead of your message")
    h.setLevel(logging.INFO)
    ClientLogging().attach(logger_name="test.test", handler=h)
    logger = logging.getLogger("test.test")
    logger.info("This is a test")
    captured = capsys.readouterr()
    logs = captured.out.split('\n')
    # should only contain two lines, one empty, one formatter text
    for log in logs:
        if log:
            assert "I log this instead of your message" in log
            assert "This is a test" not in log

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


def test_scheduler_logging(client_env, caplog):
    """Test to check that scheduler logs are sent to stdout properly."""
    from jobmon.client.api import Tool
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.requester import Requester

    # Pytest config sets log level at debug, but default in client config is INFO.
    # For a representative test we need to use INFO level.
    caplog.set_level(logging.INFO)

    t = Tool.create_tool('logging_testing_tool')
    tt = t.get_task_template(template_name='logging_tt',
                             command_template="{command}",
                             node_args=["command"])
    exec_params = ExecutorParameters(
        num_cores=1,
        m_mem_free='1Mb',
        max_runtime_seconds=10,
        executor_class='SequentialExecutor'
    )
    t1 = tt.create_task(executor_parameters=exec_params,
                        command='echo 10')
    workflow = t.create_workflow()
    workflow.set_executor(executor_class='SequentialExecutor')
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()
    requester = Requester(client_env)

    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
    assert "scheduler communicating at" in caplog.text  # From scheduler.__init__
    assert "Workflow_args defaulting to uuid" in caplog.text  # From workflow.py
    caplog.clear()  # Need to clear caplog, otherwise text persists till end of test

    # Try a purge call. Should return a warning
    scheduler._purge_queueing_errors()
    assert "does not implement get_errored_jobs methods" in caplog.text
    caplog.clear()

    # Run the scheduler in a separate process like the workflow does,
    # and check we get the same logs
    scheduler_proc = workflow._start_task_instance_scheduler(
        wfr.workflow_run_id, 180
    )
    wfr.execute_interruptible(scheduler_proc)
    assert "scheduler communicating at" in caplog.text
    assert "does not implement get_errored_jobs methods" in caplog.text
    caplog.clear()

    # Assert that log levels are implemented appropriately - no debug logs by default
    sched_logger = logging.getLogger(
        'jobmon.client.execution.scheduler.task_instance_scheduler')
    sched_logger.info("Info log")
    sched_logger.debug("Debug log")
    assert "Info log" in caplog.text
    assert "Debug log" not in caplog.text
