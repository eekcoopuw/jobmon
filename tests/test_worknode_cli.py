import pytest
from unittest.mock import patch
import sys


import jobmon.client.worker_node.cli


EXCEPTION_MSG = "assert we took this path"


class ExpectedException(Exception):
    pass


def mock_kill_remote_process_group(a, b):
    """mock the kill remote process group interface and raise an error to
    signal that we entered the path we are trying to test"""
    raise ExpectedException(EXCEPTION_MSG)


class MockIntercom:
    """Mock the intercom interface for testing purposes"""

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 hostname):
        pass

    def log_running(self, next_report_increment):
        pass

    def log_report_by(self, next_report_increment):
        pass

    def log_job_stats(self):
        pass

    def log_done(self):
        pass

    def log_error(self, error):
        if EXCEPTION_MSG in error:
            raise ExpectedException


def test_kill_remote_process_group_conditional(monkeypatch):
    # this tests the code pathway in workernode.cli to make sure that we enter
    # kill remote process group when we intend to. It does not test the kill
    # remote process group function itself.
    monkeypatch.setattr(
        jobmon.client.worker_node.cli,
        "kill_remote_process_group",
        mock_kill_remote_process_group)
    monkeypatch.setattr(
        jobmon.client.worker_node.cli,
        "JobInstanceIntercom",
        MockIntercom)

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
            jobmon.client.worker_node.cli.unwrap()

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
            jobmon.client.worker_node.cli.unwrap()
