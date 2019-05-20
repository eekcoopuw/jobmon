import subprocess

# This import is needed for the monkeypatch
from jobmon.client.swarm.executors.sge import SGEExecutorWorkerNode
from jobmon.client.worker_node.worker_node_job_instance import (
    WorkerNodeJobInstance)

error_raised = False


class BadSGEExecutor(SGEExecutorWorkerNode):
    """Mock the intercom interface for testing purposes, specifically
    to raise exceptions"""

    def get_usage_stats(self):
        global error_raised
        error_raised = True
        raise subprocess.CalledProcessError(cmd="qstat", returncode=9)


def test_bad_qstat_call(monkeypatch):
    ji_intercom = WorkerNodeJobInstance(job_instance_id=12345,
                                        executor=BadSGEExecutor(),
                                        process_group_id=9,
                                        nodename='fake_host')

    # The following should not throw
    ji_intercom.log_job_stats()
    # But check that it did
    assert error_raised
