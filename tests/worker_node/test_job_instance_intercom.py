import subprocess

# This import is needed for the monkeypatch
import jobmon.client.swarm.executors
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_instance_intercom import \
    JobInstanceIntercom

error_raised = False


class BadSGEExecutor(SGEExecutor):
    """Mock the intercom interface for testing purposes, specifically
    to raise exceptions"""

    def get_usage_stats(self):
        global error_raised
        error_raised = True
        raise subprocess.CalledProcessError(cmd="qstat", returncode=9)


def test_bad_qstat_call(monkeypatch):
    monkeypatch.setattr(
        jobmon.client.swarm.executors.sge,
        "SGEExecutor",
        BadSGEExecutor)

    ji_intercom = JobInstanceIntercom(job_instance_id=12345,
                                      executor_class=BadSGEExecutor,
                                      process_group_id=9,
                                      hostname='fake_host')

    # The following should not throw
    ji_intercom.log_job_stats()
    # But check that it did
    assert error_raised
