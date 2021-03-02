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
    id, s = wfr._link_to_workflow()
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING
    # get current heartbeat
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = "SELECT heartbeat_date " \
                "FROM workflow_run " \
                "WHERE id={} ".format(id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    current_hb = res[0]
    wfr._log_heartbeat()
    with app.app_context():
        query = "SELECT heartbeat_date " \
                "FROM workflow_run " \
                "WHERE id={} ".format(id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    new_hb = res[0]
    assert new_hb > current_hb
    assert s == WorkflowRunStatus.LINKING
    assert wf._status == WorkflowStatus.REGISTERING

