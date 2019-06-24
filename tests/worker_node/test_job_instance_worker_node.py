import pkg_resources
import pytest
import shutil
import subprocess
import sys
from unittest.mock import patch

# This import is needed for the monkeypatch
from jobmon.client import shared_requester, client_config
from jobmon.client.swarm.executors.base import Executor
from jobmon.client.swarm.executors.sge import JobInstanceSGEInfo
import jobmon.client.swarm.job_management.job_instance_factory
import jobmon.client.swarm.job_management.executor_job_instance
from jobmon.client.worker_node.worker_node_job_instance import (
    WorkerNodeJobInstance)
import jobmon.client.worker_node.execution_wrapper
import jobmon.client.worker_node.worker_node_job_instance
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask

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
    assert exit_code.value.code == 555


def mock_wrapped_command(self, command: str, job_instance_id: int,
                          last_nodename = None,
                          last_process_group_id = None) -> str:
    jobmon_command = client_config.jobmon_command
    if not jobmon_command:
        jobmon_command = shutil.which("jobmon_command")
    wrapped_cmd = [
        jobmon_command,
        "--command", f"'{command}'",
        "--job_instance_id", job_instance_id,
        "--expected_jobmon_version", 'wrong_version',
        "--executor_class", self.__class__.__name__,
        "--heartbeat_interval", client_config.heartbeat_interval,
        "--report_by_buffer", client_config.report_by_buffer
    ]
    if self.temp_dir and 'stata' in command:
        wrapped_cmd.extend(["--temp_dir", self.temp_dir])
    if last_nodename:
        wrapped_cmd.extend(["--last_nodename", last_nodename])
    if last_process_group_id:
        wrapped_cmd.extend(["--last_pgid", last_process_group_id])
    str_cmd = " ".join([str(i) for i in wrapped_cmd])
    return str_cmd


def test_workflow_wrong_jobmon_versions(monkeypatch, db_cfg, real_dag_id):
    """
    check database to make sure correct information is propogated back
    """

    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.executors import Executor
    from jobmon.client.swarm.job_management.job_list_manager import JobListManager
    monkeypatch.setattr(Executor, 'build_wrapped_command', mock_wrapped_command)
    task = BashTask("sleep 2", num_cores=1)
    executor = SGEExecutor(project='proj_tools')
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=False, interrupt_on_error=False)
    job = jlm.bind_task(task)
    jlm.queue_job(job)
    instantiated = jlm.job_instance_factory.instantiate_queued_jobs()
    jid = instantiated[0]
    status = query_until_error(db_cfg, jid)
    while status[0] != 'U':
        status = query_until_error(db_cfg, jid)
        jlm.job_inst_reconciler._account_for_lost_job_instances()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query="""SELECT description 
                 FROM job_instance_error_log 
                 WHERE job_instance_id={jid}""".format(jid=jid)
        description = DB.session.execute(query).fetchone()
        DB.session.commit()
        assert 'There is a discrepancy between the environment' in description [0]
    jlm.disconnect()

def query_until_error(db_cfg, jid):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
                   SELECT status
                   FROM job_instance
                   WHERE job_instance_id = {jid}
               """.format(jid=jid)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    return res



