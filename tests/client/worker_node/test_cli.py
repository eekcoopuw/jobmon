import os
import sys
from unittest.mock import patch
import pkg_resources
import pytest

import jobmon.client.execution.worker_node.execution_wrapper
import jobmon.client.execution.worker_node.worker_node_task_instance
import jobmon.client.execution.strategies.sequential


@pytest.mark.unittest
def test_executor_id_from_env():
    """ test if the environment variable JOB_ID can be passed to sequential.TaskInstanceSequentialInfo"""
    with patch.dict(os.environ, {'JOB_ID': '77777'}):
        assert jobmon.client.execution.strategies.sequential.TaskInstanceSequentialInfo().executor_id == 77777


def test_unwrap_happy_path():
    with patch.dict(os.environ,{'JOB_ID':'77777'}), \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_done") as m_done, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_running") as m_run, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_report_by") as m_by, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_error") as m_err:

        m_done.return_value = None
        m_run.return_value = (200, False)
        m_by.return_value = None
        m_err.return_value = None
        r = jobmon.client.execution.worker_node.execution_wrapper.unwrap(task_instance_id=1,
                                                                         command="printenv",
                                                                         expected_jobmon_version=pkg_resources.get_distribution("jobmon").version,
                                                                         executor_class="SequentialExecutor",
                                                                         heartbeat_interval=1,
                                                                         report_by_buffer=3.1)
        assert r == 0


def test_stderr_buffering(capsys):
    # this test checks 2 things.
    # 1) that we are getting logging messages from both jobmon and the
    # subprocess intermixed. This implies zero latency in stderr
    # 2) it checks that we can send large amounts of data to stderr without
    # causing a deadlock. the script 'fill_pipe.py' sends "a" * 2**10 * 2**8
    # to sdterr
    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
    def mock_log_report_by(next_report_increment):
        print("logging report by in the middle", file=sys.stderr)

    def mock_log_error(error_message, exit_status):
        assert error_message == ("a" * 2**10 + "\n") * (2**8)

    with patch.dict(os.environ, {'JOB_ID': '77777'}), \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_done") as m_done, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_running") as m_run, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_report_by") as m_by, \
         patch("jobmon.client.execution.worker_node.worker_node_task_instance.WorkerNodeTaskInstance.log_error") as m_err:

        m_done.return_value = None
        m_run.return_value = (200, False)
        m_by.side_effect = mock_log_report_by
        m_err.side_effect = mock_log_error
        r = jobmon.client.execution.worker_node.execution_wrapper.unwrap(task_instance_id=1,
                                                                         command=f"python {os.path.join(thisdir, 'fill_pipe.py')}",
                                                                         expected_jobmon_version=pkg_resources.get_distribution(
                                                                         "jobmon").version,
                                                                         executor_class="SequentialExecutor",
                                                                         heartbeat_interval=1,
                                                                         report_by_buffer=3.1)
        assert r == 1
        captured = capsys.readouterr()

        members = captured.err.split("logging report by in the middle\n")
        assert len(members) > 5  # should be report_bys in the middle of the aaaa's

        # confirm we got all stderr from child
        aaaa = ""
        for block in members:
            aaaa += block
        assert aaaa == ("a" * 2**10 + "\n") * (2**8)
