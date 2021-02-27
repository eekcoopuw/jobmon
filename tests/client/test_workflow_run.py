from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.constants import WorkflowRunStatus, WorkflowStatus

import pytest

def test_log_heartbeat(client_env, db_cfg):
    """test _log_heartbeat sets the wfr status to L
    """
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.workflow_run import WorkflowRun

    # Create identical dags
    t1 = BashTask("sleep 1")
    wf = UnknownWorkflow("v1")
    wf.add_tasks([t1])
    wf.bind()
    wfr = WorkflowRun(
            workflow_id=wf._workflow_id,
            executor_class=wf._executor.__class__.__name__,
            requester=wf.requester
        )
    wfr.bind(tasks={1: t1})
    assert wfr.status == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING

