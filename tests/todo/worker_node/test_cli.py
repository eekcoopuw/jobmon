import os
import sys
from unittest.mock import patch
import pkg_resources
import pytest

import jobmon.client.worker_node.execution_wrapper

EXCEPTION_MSG = "assert we took this path"

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


class ExpectedException(Exception):
    pass


class MockIntercom:
    """Mock the intercom interface for testing purposes"""

    def __init__(self, job_instance_id, job_instance_executor_info):
        self.job_instance_id = job_instance_id
        self.executor = job_instance_executor_info

    @property
    def executor_id(self):
        return self.executor.executor_id

    def log_running(self, next_report_increment):
        return 200, False

    def log_report_by(self, next_report_increment):
        pass

    def log_job_stats(self):
        pass

    def log_done(self):
        pass

    def log_error(self, error_message, exit_status):
        pass

    def in_kill_self_state(self):
        return False


def mock_run_remote_command(hostname: str, command: str):
    """mock the kill remote process group interface and raise an error to
    signal that we entered the path we are trying to test"""
    raise ExpectedException(f"{EXCEPTION_MSG}; {hostname}: {command}")


class MockIntercomRaiseInLogError(MockIntercom):

    def log_error(self, error_message, exit_status):
        if EXCEPTION_MSG in error_message:
            raise ExpectedException


class MockIntercomCheckExecutorId(MockIntercom):

    def log_running(self, next_report_increment):
        assert self.executor_id == 77777
        return 200, False

    def log_report_by(self, next_report_increment):
        assert self.executor_id == 77777

    def log_done(self):
        assert self.executor_id == 77777

    def log_error(self, error_message, exit_status):
        assert self.executor_id == 77777


class MockIntercomLogHeartbeatToError(MockIntercom):

    def log_report_by(self, next_report_increment):
        print("logging report by in the middle", file=sys.stderr)

    def log_error(self, error_message, exit_status):
        assert error_message == ("a" * 2**10 + "\n") * (2**8)


def test_stderr_buffering(monkeypatch, capsys):
    # this test checks 2 things.
    # 1) that we are getting logging messages from both jobmon and the
    # subprocess intermixed. This implies zero latency in stderr
    # 2) it checks that we can send large amounts of data to stderr without
    # causing a deadlock. the script 'fill_pipe.py' sends "a" * 2**10 * 2**8
    # to sdterr

    monkeypatch.setattr(
        jobmon.client.worker_node.execution_wrapper, "WorkerNodeJobInstance",
        MockIntercomLogHeartbeatToError)

    # arguments in the structure that jobmon.client.worker_node.cli.unwrap()
    # usually recieves from the command line
    base_args = [
        "fakescript",
        "--command", f"python {os.path.join(thisdir, 'fill_pipe.py')}",
        "--job_instance_id", "1",
        "--expected_jobmon_version", pkg_resources.get_distribution("jobmon").version,
        "--executor_class", "SequentialExecutor",
        "--heartbeat_interval", "1",
        "--report_by_buffer", "3.1"
    ]

    # this call should raise a SystemExit because we don't evaluate the
    # kill_remote_process_group block and hence won't raise any errors
    with patch.object(sys, 'argv', base_args):
        with pytest.raises(SystemExit):
            jobmon.client.worker_node.execution_wrapper.unwrap()
    captured = capsys.readouterr()
    members = captured.err.split("logging report by in the middle\n")
    assert len(members) > 5  # should be report_bys in the middle of the aaaa's

    # confirm we got all stderr from child
    aaaa = ""
    for block in members:
        aaaa += block
    assert aaaa == ("a" * 2**10 + "\n") * (2**8)


def test_executor_id(monkeypatch, capsys):
    """ this test is checking that the jobmon cli can access its own job id
    to send in the routes it is logging"""
    monkeypatch.setattr(
        jobmon.client.worker_node.execution_wrapper,
        "WorkerNodeJobInstance",
        MockIntercomCheckExecutorId)

    monkeypatch.setenv("JOB_ID", '77777')

    base_args = [
        "executor_id",
        "--command", "printenv",
        "--job_instance_id", "1",
        "--expected_jobmon_version", pkg_resources.get_distribution("jobmon").version,
        "--executor_class", "SequentialExecutor",
        "--heartbeat_interval", "1",
        "--report_by_buffer", "3.1"
    ]

    with patch.object(sys, 'argv', base_args):
        with pytest.raises(SystemExit):
            jobmon.client.worker_node.execution_wrapper.unwrap()
