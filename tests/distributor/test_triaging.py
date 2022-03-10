import time


import pytest
import random
from typing import Any, Dict

from jobmon.requester import Requester
from jobmon.constants import TaskInstanceStatus


@pytest.fixture
def tool(db_cfg, client_env):
    from jobmon.client.tool import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


class MockDistributorProc:
    def __init__(self):
        pass

    def is_alive(self) -> bool:
        return True


@pytest.mark.skip
def test_unknown_state(tool, db_cfg, client_env, task_template, monkeypatch):
    """Creates a job instance, gets an distributor id so it can be in submitted
    to the batch distributor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.dummy import DummyDistributor

    class TestDummyDistributor(DummyDistributor):
        """a test DummyDistributor that bypasses the setting of log_running and log_done"""

        def __init__(self):
            """init the same way as DummyDistributor."""
            super().__init__()

        def submit_to_batch_distributor(
            self, command: str, name: str, requested_resources: Dict[str, Any]
        ) -> int:
            """Run a fake execution of the task.
            In a real executor, this is where qsub would happen.
            here, since it's a dummy executor, we just get a random num
            """
            distributor_id = random.randint(1, int(1e7))

            # Not setting anything up for this reconcilliation test

            return distributor_id

    task_instance_heartbeat_interval = 5

    task = task_template.create_task(
        arg="ls",
        name="dummyfbb",
        max_attempts=1,
        cluster_name="dummy",
        compute_resources={"queue": "null.q"},
    )
    workflow = tool.create_workflow(name="foo")

    workflow.add_task(task)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    test_dummy_distributor = TestDummyDistributor()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        test_dummy_distributor,
        requester=requester,
        task_instance_heartbeat_interval=task_instance_heartbeat_interval,
    )
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
    time.sleep(
        distributor_service._task_instance_heartbeat_interval
        * (distributor_service._report_by_buffer + 1)
    )

    # job will move into lost track because it never logs a heartbeat
    distributor_service._get_lost_task_instances()
    assert len(distributor_service._to_reconcile) == 1

    # will check the distributor's return state and move the job to unknown
    distributor_service.distribute()
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "U"


def test_triaging_on_multiprocess(tool, db_cfg, client_env, task_template):
    """tests that a task can be triaged and log as unknown error"""
    import time
    from unittest import mock
    from datetime import datetime
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor

    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )

    t1 = task_template.create_task(arg="sleep 100")
    t2 = task_template.create_task(arg="sleep 101")
    workflow = tool.create_workflow(name="test_triaging_on_multiprocess")

    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)
    swarm.process_commands()

    distributor = MultiprocessDistributor(5)
    distributor.start()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        distributor,
        requester=workflow.requester,
        raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    # check the job turned into I
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "I"
    assert res[1][1] == "I"

    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check the job to be Launched
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "O"
    assert res[1][1] == "O"

    distributor_service.process_status(TaskInstanceStatus.LAUNCHED)

    # turn the 2 task instances from LAUNCHED to TRIAGING in 2 steps:
    # 1. stage the report_by_date to be 1 hour past
    with app.app_context():
        sql = """
        UPDATE task_instance
        SET report_by_date = CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        WHERE task_id in :task_ids"""
        DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]})
        DB.session.commit()
    # 2. call swarm._log_triaging() to change the status to TRIAGING
    swarm._log_triaging()

    # check the jobs to be Triaging
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "T"
    assert res[1][1] == "T"

    with mock.patch(
        "jobmon.cluster_type.multiprocess.multiproc_distributor."
        "MultiprocessDistributor.get_remote_exit_info",
            return_value=(TaskInstanceStatus.UNKNOWN_ERROR, "Unknown error")
    ):

        # code logic to test
        distributor_service.process_status(TaskInstanceStatus.TRIAGING)

        # check the jobs to be UNKNOWN_ERROR as expected
        with app.app_context():
            sql = """
            SELECT ti.status, tiel.description
            FROM task_instance ti
                JOIN task_instance_error_log tiel ON ti.id = tiel.task_instance_id
            WHERE ti.task_id in :task_ids"""
            res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
            DB.session.commit()

        assert len(res) == 2
        assert res[0][0] == TaskInstanceStatus.UNKNOWN_ERROR
        assert res[0][1] == "Unknown error"
        assert res[1][0] == TaskInstanceStatus.UNKNOWN_ERROR
        assert res[1][1] == "Unknown error"

    distributor.stop()
