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


def test_set_status_for_triaging(tool, db_cfg, client_env, task_template):
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

    tis = [task_template.create_task(arg="sleep 10" + str(x)) for x in range(2)]
    workflow = tool.create_workflow(name="test_set_status_for_no_heartbeat")

    workflow.add_tasks(tis)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)
    swarm.process_commands()

    app = db_cfg["app"]
    DB = db_cfg["DB"]

    distributor = MultiprocessDistributor(5)
    distributor.start()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        distributor,
        requester=workflow.requester,
        raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)

    # turn the 3 task instances in different testing paths
    # 1. stage the report_by_date, along with respective status
    with app.app_context():
        params = {
            "launched_status": TaskInstanceStatus.LAUNCHED,
            "running_status": TaskInstanceStatus.RUNNING,
            "task_id_1": tis[0].task_id,
            "task_id_2": tis[1].task_id,
            "task_ids": [tis[x].task_id for x in range(len(tis))]
        }
        sql = """
        UPDATE task_instance
        SET report_by_date = CURRENT_TIMESTAMP() - INTERVAL 1 HOUR,
            status =
                CASE
                    WHEN task_id = :task_id_1 THEN :launched_status
                    WHEN task_id = :task_id_2 THEN :running_status
                END
        WHERE task_id in :task_ids"""
        DB.session.execute(sql, params)
        DB.session.commit()
    # 2. call swarm._set_status_for_triaging()
    swarm._set_status_for_triaging()

    # check the jobs to be Triaging
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [tis[x].task_id for x in range(len(tis))]}).all()
        DB.session.commit()

    assert len(res) == len(tis)
    assert res[0][1] == TaskInstanceStatus.KILL_SELF
    assert res[1][1] == TaskInstanceStatus.TRIAGING

    distributor.stop()


def test_triaging_to_error_on_multiprocess(tool, db_cfg, client_env, task_template):

    def test_triaging_to_specific_error(tool, db_cfg, client_env, task_template,
                                        error_message):
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

        tis = [task_template.create_task(arg="sleep 6" + str(x)) for x in range(2)]
        workflow = tool.create_workflow(name="test_triaging_on_multiprocess")

        workflow.add_tasks(tis)
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
            res = DB.session.execute(sql,
                                     {"task_ids": [t.task_id for t in tis]}
                                     ).all()
            DB.session.commit()

        assert len(res) == len(tis)
        assert [res[x][1] for x in range(len(tis))] == ["I" for _ in range(len(tis))]

        distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

        # check the job to be Launched
        with app.app_context():
            sql = """
            SELECT id, task_instance.status
            FROM task_instance
            WHERE task_id in :task_ids
            ORDER BY id"""
            res = DB.session.execute(sql,
                                     {"task_ids": [t.task_id for t in tis]}
                                     ).all()
            DB.session.commit()

        assert len(res) == len(tis)
        assert [res[x][1] for x in range(len(tis))] == ["O" for _ in range(len(tis))]

        # stage all the task instances as triaging
        with app.app_context():
            params = {
                "triaging_status": TaskInstanceStatus.TRIAGING,
                "task_ids": [tis[x].task_id for x in range(len(tis))]
            }
            sql = """
            UPDATE task_instance
            SET report_by_date = CURRENT_TIMESTAMP() - INTERVAL 1 HOUR,
                status = :triaging_status
            WHERE task_id in :task_ids"""
            DB.session.execute(sql, params)
            DB.session.commit()

        # synchronize statuses from the db and get new work
        distributor_service._check_for_work(TaskInstanceStatus.TRIAGING)

        with mock.patch(
            "jobmon.cluster_type.multiprocess.multiproc_distributor."
            "MultiprocessDistributor.get_remote_exit_info",
                return_value=error_message
        ):

            # code logic to test
            distributor_service.process_status(TaskInstanceStatus.TRIAGING)

            # check the jobs to be UNKNOWN_ERROR as expected
            with app.app_context():
                sql = """
                SELECT ti.id, ti.status, tiel.description
                FROM task_instance ti
                    JOIN task_instance_error_log tiel ON ti.id = tiel.task_instance_id
                WHERE ti.task_id in :task_ids
                GROUP BY ti.id, ti.status, tiel.description"""
                res = DB.session.execute(sql, {"task_ids": [t.task_id for t in tis]}).all()
                DB.session.commit()

            assert len(res) == len(tis)
            assert [(res[x][1], res[x][2]) for x in range(len(res))] == \
                   [error_message for _ in range(len(res))]

        distributor.stop()

    test_triaging_to_specific_error(tool, db_cfg, client_env, task_template,
                                    (TaskInstanceStatus.RESOURCE_ERROR,
                                     "Insufficient resources requested. Task was lost"))
    test_triaging_to_specific_error(tool, db_cfg, client_env, task_template,
                                    (TaskInstanceStatus.UNKNOWN_ERROR,
                                     "One possible reason might be that either stderr "
                                     "or stdout is not accessible or writable"))
    test_triaging_to_specific_error(tool, db_cfg, client_env, task_template,
                                    (TaskInstanceStatus.ERROR_FATAL, "Error Fatal occurred"))
