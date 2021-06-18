import os
import sys
from unittest.mock import patch

from jobmon.cluster_type import sequential
from jobmon.worker_node import worker_node_task_instance

import pkg_resources

import pytest


# module paths
worker_node = "jobmon.client.distributor.worker_node."
WNTI = "worker_node_task_instance.WorkerNodeTaskInstance."


def mock_kill_self(*args, **kwargs):
    # The real kill_self sends a kill -9 and sometimes kill the pytest process;
    # thus, mock it
    pass


@pytest.mark.unittest
def test_executor_id_from_env():
    """ test if the environment variable JOB_ID can be passed to
    sequential.TaskInstanceSequentialInfo"""
    with patch.dict(os.environ, {'JOB_ID': '77777'}):
        assert jobmon.client.execution.strategies.sequential.\
            TaskInstanceSequentialInfo().executor_id == 77777


def test_unwrap_happy_path(client_env):

    with \
            patch.dict(os.environ, {'JOB_ID': '77777'}), \
            patch(worker_node + WNTI + "log_done") as m_done, \
            patch(worker_node + WNTI + "log_running") as m_run, \
            patch(worker_node + WNTI + "log_report_by") as m_by, \
            patch(worker_node + WNTI + "log_error") as m_err, \
            patch(worker_node + WNTI + "in_kill_self_state") as m_kill_self, \
            patch(worker_node + "execution_wrapper.kill_self") as m_kill:
        m_kill_self.return_value = False
        m_kill.side_effect = mock_kill_self
        m_done.return_value = None
        m_run.return_value = (200, False)
        m_by.return_value = None
        m_err.return_value = None

        r = jobmon.client.execution.worker_node.execution_wrapper.unwrap(
             task_instance_id=1,
             command="printenv",
             expected_jobmon_version=pkg_resources.get_distribution(
                "jobmon").version,
             executor_class="SequentialExecutor",
             heartbeat_interval=1,
             report_by_buffer=3.1)
        assert r == 0


def test_stderr_buffering(capsys, client_env):
    # this test checks 2 things.
    # 1) that we are getting logging messages from both jobmon and the
    # subprocess intermixed. This implies zero latency in stderr
    # 2) it checks that we can send large amounts of data to stderr without
    # causing a deadlock. the script 'fill_pipe.py' sends "a" * 2**10 * 2**8
    # to sdterr
    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
    with \
            patch.dict(os.environ, {'JOB_ID': '77777'}), \
            patch(worker_node + WNTI + "log_done") as m_done, \
            patch(worker_node + WNTI + "log_running") as m_run, \
            patch(worker_node + WNTI + "log_report_by") as m_by, \
            patch(worker_node + WNTI + "log_error") as m_err, \
            patch(worker_node + WNTI + "in_kill_self_state") as m_kill_self, \
            patch(worker_node + "execution_wrapper.kill_self") as m_kill:

        def mock_log_report_by(next_report_increment):
            print("logging report by in the middle", file=sys.stderr)

        def mock_log_error(error_message, exit_status):
            assert error_message == ("a" * 2 ** 10 + "\n") * (2 ** 8)

        m_kill_self.return_value = False
        m_kill.side_effect = mock_kill_self
        m_done.return_value = None
        m_run.return_value = (200, False)
        m_by.side_effect = mock_log_report_by
        m_err.side_effect = mock_log_error
        r = jobmon.client.execution.worker_node.execution_wrapper.unwrap(
             task_instance_id=1,
             command=f"python {os.path.join(thisdir, 'fill_pipe.py')}",
             expected_jobmon_version=pkg_resources.get_distribution(
                "jobmon").version,
             executor_class="SequentialExecutor",
             heartbeat_interval=1,
             report_by_buffer=3.1)
        assert r == 1
        captured = capsys.readouterr()
        cap_str = captured.err
        # Log sometimes insert unwanted things to the output; thus, just count
        # "a"
        assert cap_str.count("a") >= (("a" * 2**10 + "\n") * (2**8)).count("a")
