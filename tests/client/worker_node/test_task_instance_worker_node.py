from unittest.mock import patch
import pytest
import subprocess

from jobmon.client.execution.strategies.sge import TaskInstanceSGEInfo
from jobmon.client.execution.worker_node.worker_node_task_instance import WorkerNodeTaskInstance
from jobmon.exceptions import ReturnCodes
from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor
from jobmon.constants import TaskInstanceStatus
from jobmon.client.execution.strategies.base import ExecutorParameters

error_raised = False


class BadSGEExecutor(TaskInstanceSGEInfo):
    """Mock the intercom interface for testing purposes, specifically
    to raise exceptions"""
    def get_usage_stats(self):
        global error_raised
        error_raised = True
        raise subprocess.CalledProcessError(cmd="qstat", returncode=9)


def test_bad_qstat_call(client_env):
    ji_intercom = WorkerNodeTaskInstance(
        task_instance_id=12345, task_instance_executor_info=BadSGEExecutor()
    )
    # The following should not throw
    ji_intercom.log_task_stats()
    # But check that it did
    assert error_raised


@pytest.mark.unittest
def test_wrong_jobmon_versions_get_remote_exit_info():
    """
    mock the qacct_exit_status response
    """
    expected_words = "There is a discrepancy between the environment"
    with patch("jobmon.client.execution.strategies.sge.sge_utils.qacct_exit_status") as m_exit_code:
        m_exit_code.return_value = (ReturnCodes.WORKER_NODE_ENV_FAILURE, "I am making this up.")
        executor = SGEExecutor()
        r_value, r_msg = executor.get_remote_exit_info(1)
        assert r_value == TaskInstanceStatus.ERROR_FATAL
        assert expected_words in r_msg


@pytest.mark.unittest
def test_wrong_jobmon_version_execute(db_cfg, client_env):
    """
    mock the _execute_sge
    """
    expected_words = "There is a discrepancy between the environment"
    with patch("jobmon.client.execution.strategies.sge.sge_executor.SGEExecutor._execute_sge") as m_execute_sge, \
            patch("jobmon.client.execution.strategies.sge.sge_utils.qacct_exit_status") as m_exit_code:
        m_execute_sge.return_value = 123456
        m_exit_code.return_value = (ReturnCodes.WORKER_NODE_ENV_FAILURE, "I am making this up again.")
        executor = SGEExecutor()
        executor.jobmon_command = "jobmon"
        executor.execute("date", "my execution", ExecutorParameters())
        r_value, r_msg = executor.get_remote_exit_info(1)
        assert r_value == TaskInstanceStatus.ERROR_FATAL
        assert expected_words in r_msg
