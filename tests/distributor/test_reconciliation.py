import time

from jobmon.requester import Requester

import pytest
import random
from typing import Any, Dict, Optional

from jobmon.cluster_type.dummy import DummyDistributor
from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(cluster_name="sequential",
                                                 compute_resources={"queue": "null.q"})
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


class MockDistributorProc:

    def __init__(self):
        pass

    def is_alive(self) -> bool:
        return True


def test_unknown_state(tool, db_cfg, client_env, task_template, monkeypatch):
    """Creates a job instance, gets an distributor id so it can be in submitted
    to the batch distributor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    class TestDummyDistributor(DummyDistributor):
        """ a test DummyDistributor that bypasses the setting of log_running and log_done"""
        def __init__(self):
            """init the same way as DummyDistributor."""
            super().__init__()

        def submit_to_batch_distributor(self, command: str, name: str,
                                        requested_resources: Dict[str, Any]) -> int:
            """Run a fake execution of the task.
            In a real executor, this is where qsub would happen.
            here, since it's a dummy executor, we just get a random num
            """
            distributor_id = random.randint(1, int(1e7))

            # Not setting anything up for this reconcilliation test

            return distributor_id

    task_instance_heartbeat_interval = 5

    task = task_template.create_task(
        arg="ls", name="dummyfbb", max_attempts=1,
        cluster_name="dummy", compute_resources={"queue": "null.q"})
    workflow = tool.create_workflow(name="foo")

    workflow.add_task(task)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    test_dummy_distributor = TestDummyDistributor()

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             test_dummy_distributor, requester=requester,
                                             task_instance_heartbeat_interval=task_instance_heartbeat_interval)
    distributor_service.distribute()

    # Since we are using the 'dummy' distributor, we never actually do
    # any work. The job gets moved to lost_track during reconciliation
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = """
        SELECT task_instance.status
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "B"

    # sleep through the report by date
    time.sleep(distributor_service._task_instance_heartbeat_interval
               * (distributor_service._report_by_buffer + 1))

    # job will move into lost track because it never logs a heartbeat
    distributor_service._get_lost_task_instances()
    assert len(distributor_service._to_reconcile) == 1

    # will check the distributor's return state and move the job to unknown
    distributor_service.distribute()
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "U"


def test_log_distributor_report_by(tool, db_cfg, client_env, task_template, monkeypatch):
    """test that jobs that are queued by an distributor but not running still log
    heartbeats"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    # patch unwrap from sequential so the command doesn't execute,
    # def mock_unwrap(*args, **kwargs):
    #     pass
    # monkeypatch.setattr(sequential, "unwrap", mock_unwrap)

    task = task_template.create_task(
        arg="sleep 5", name="heartbeat_sleeper",
        compute_resources={"queue": "null.q", "cores": 1, "max_runtime_seconds": 500},
        cluster_name="sequential")
    workflow = tool.create_workflow(name="foo")
    workflow.add_task(task)

    # add workflow info to db and then time out.
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             SequentialDistributor(), requester=requester)

    # instantiate the job and then log a report by
    distributor_service.distribute()
    distributor_service._log_distributor_report_by()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.submitted_date, task_instance.report_by_date
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
        res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged
