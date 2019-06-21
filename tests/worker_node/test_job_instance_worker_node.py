import pkg_resources
import pytest
import subprocess
import sys
from unittest.mock import patch

# This import is needed for the monkeypatch
from jobmon.client import shared_requester, client_config
from jobmon.client.swarm.executors.base import Executor
from jobmon.client.swarm.executors.sge import JobInstanceSGEInfo
from jobmon.client.worker_node.worker_node_job_instance import (
    WorkerNodeJobInstance)
import jobmon.client.worker_node.execution_wrapper
import jobmon.client.worker_node.worker_node_job_instance

error_raised = False


class BadSGEExecutor(JobInstanceSGEInfo):
    """Mock the intercom interface for testing purposes, specifically
    to raise exceptions"""

    def get_usage_stats(self):
        global error_raised
        error_raised = True
        raise subprocess.CalledProcessError(cmd="qstat", returncode=9)


def test_bad_qstat_call(monkeypatch):
    ji_intercom = WorkerNodeJobInstance(
        job_instance_id=12345, job_instance_executor_info=BadSGEExecutor(),
        expected_jobmon_version=pkg_resources.get_distribution("jobmon").version)

    # The following should not throw
    ji_intercom.log_job_stats()
    # But check that it did
    assert error_raised


class MockWorkerNodeJobInstance(WorkerNodeJobInstance):
    def log_error(self, error_message, exit_status):
        return 200


def test_wrong_jobmon_versions(monkeypatch):
    monkeypatch.setattr(jobmon.client.worker_node.execution_wrapper,
                        "WorkerNodeJobInstance", MockWorkerNodeJobInstance)
    version = 'wrong_version'
    base_args = [
        "fakescript",
        "--command", "ls",
        "--job_instance_id", "1",
        "--expected_jobmon_version", version,
        "--executor_class", "SequentialExecutor",
        "--heartbeat_interval", "90",
        "--report_by_buffer", "3.1"
    ]
    with patch.object(sys, 'argv', base_args):
        with pytest.raises(SystemExit) as exit_code:
            jobmon.client.worker_node.execution_wrapper.unwrap()
    assert exit_code.value.code == 198
