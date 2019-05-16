import os
import sys
from unittest.mock import patch

import pytest

import jobmon.client.worker_node.execution_wrapper

EXCEPTION_MSG = "assert we took this path"

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


class ExpectedException(Exception):
    pass


class MockIntercom:
    """Mock the intercom interface for testing purposes"""

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 hostname):
        pass

    def log_running(self, next_report_increment, executor_id):
        pass

    def log_report_by(self, next_report_increment, executor_id):
        pass

    def log_job_stats(self):
        pass

    def log_done(self, executor_id):
        pass

    def log_error(self, error_message, executor_id, exit_status):
        pass


def mock_kill_remote_process_group(a, b):
    """mock the kill remote process group interface and raise an error to
    signal that we entered the path we are trying to test"""
    raise ExpectedException(EXCEPTION_MSG)


class MockIntercomRaiseInLogError(MockIntercom):

    def log_error(self, error_message, executor_id, exit_status):
        if EXCEPTION_MSG in error_message:
            raise ExpectedException


class MockIntercomCheckExecutorId(MockIntercom):

    def log_running(self, next_report_increment, executor_id):
        assert executor_id == '77777'

    def log_report_by(self, next_report_increment, executor_id):
        assert executor_id == '77777'

    def log_done(self, executor_id):
        assert executor_id == '77777'

    def log_error(self, error_message, executor_id, exit_status):
        assert executor_id == '77777'


def test_kill_remote_process_group_conditional(monkeypatch):
    # this tests the code pathway in workernode.cli to make sure that we enter
    # kill remote process group when we intend to. It does not test the kill
    # remote process group function itself.
    monkeypatch.setattr(
        jobmon.client.worker_node.execution_wrapper,
        "kill_remote_process_group",
        mock_kill_remote_process_group)
    monkeypatch.setattr(
        jobmon.client.worker_node.execution_wrapper,
        "WorkerNodeIntercom",
        MockIntercomRaiseInLogError)

    # arguments in the structure that jobmon.client.worker_node.cli.unwrap()
    # usually recieves from the command line
    base_args = [
        "fakescript",
        "--command", "ls",
        "--job_instance_id", "1",
        "--jm_host", "some.host.name",
        "--jm_port", "2",
        "--executor_class", "DummyExecutor",
        "--heartbeat_interval", "90",
        "--report_by_buffer", "3.1"
    ]

    # this call should raise a SystemExit because we don't evaluate the
    # kill_remote_process_group block and hence won't raise any errors
    with patch.object(sys, 'argv', base_args):
        with pytest.raises(SystemExit):
            jobmon.client.worker_node.execution_wrapper.unwrap()

    # this call to unwrap() should raise a TestException because we patched the
    # old kill_remote_process_group with a fake one that raises an exception
    # and then forced it to re-raise outside of the try block using a fake
    # "log_error"
    process_group_args = [
        "--last_nodename", "other.fake.host",
        "--last_pgid", "3"
    ]
    with patch.object(sys, 'argv', base_args + process_group_args):
        with pytest.raises(ExpectedException):
            jobmon.client.worker_node.execution_wrapper.unwrap()


class MockIntercomLogHeartbeatToError(MockIntercom):

    def log_report_by(self, next_report_increment, executor_id):
        print("logging report by in the middle", file=sys.stderr)

    def log_error(self, error_message, executor_id, exit_status):
        assert error_message == ("a" * 2**10 + "\n") * (2**8)


def test_stderr_buffering(monkeypatch, capsys):
    # this test checks 2 things.
    # 1) that we are getting logging messages from both jobmon and the
    # subprocess intermixed. This implies zero latency in stderr
    # 2) it checks that we can send large amounts of data to stderr without
    # causing a deadlock. the script 'fill_pipe.py' sends "a" * 2**10 * 2**8
    # to sdterr

    monkeypatch.setattr(
        jobmon.client.worker_node.execution_wrapper, "WorkerNodeIntercom",
        MockIntercomLogHeartbeatToError)

    # arguments in the structure that jobmon.client.worker_node.cli.unwrap()
    # usually recieves from the command line
    base_args = [
        "fakescript",
        "--command", f"python {os.path.join(thisdir, 'fill_pipe.py')}",
        "--job_instance_id", "1",
        "--jm_host", "some.host.name",
        "--jm_port", "2",
        "--executor_class", "DummyExecutor",
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
        "WorkerNodeIntercom",
        MockIntercomCheckExecutorId)

    monkeypatch.setenv("JOB_ID", '77777')

    base_args = [
        "executor_id",
        "--command", "printenv",
        "--job_instance_id", "1",
        "--jm_host", "some.host.name",
        "--jm_port", "2",
        "--executor_class", "DummyExecutor",
        "--heartbeat_interval", "1",
        "--report_by_buffer", "3.1"
    ]

    with patch.object(sys, 'argv', base_args):
        with pytest.raises(SystemExit):
            jobmon.client.worker_node.execution_wrapper.unwrap()
