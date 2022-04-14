import pytest
from sqlalchemy.sql import text

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


def test_set_status_for_triaging(tool, db_cfg, client_env, task_template):
    """tests that a task can be triaged and log as unknown error"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
    from jobmon.server.web.models.task_instance import TaskInstance

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
    swarm.set_initial_fringe()
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
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[ti.task_id for ti in tis])
            .all()
        )
        DB.session.commit()

    assert len(res) == len(tis)
    assert res[0].status == TaskInstanceStatus.KILL_SELF
    assert res[1].status == TaskInstanceStatus.TRIAGING

    distributor.stop()


@pytest.mark.parametrize(
    "error_state, error_message",
    [(TaskInstanceStatus.RESOURCE_ERROR, "Insufficient resources requested. Task was lost"),
     (TaskInstanceStatus.UNKNOWN_ERROR,
      "One possible reason might be that either stderr or stdout is not accessible or writable"
      ),
     (TaskInstanceStatus.ERROR_FATAL, "Error Fatal occurred")
     ],
)
def test_triaging_to_specific_error(tool, db_cfg, client_env, task_template,
                                    error_state, error_message):
    """tests that a task can be triaged and log as unknown error"""
    from unittest import mock
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
    from jobmon.server.web.models.task_instance import TaskInstance

    app = db_cfg["app"]
    DB = db_cfg["DB"]

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
    swarm.set_initial_fringe()
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
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

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
    # distributor_service._check_for_work(TaskInstanceStatus.TRIAGING)

    with mock.patch(
        "jobmon.cluster_type.multiprocess.multiproc_distributor."
        "MultiprocessDistributor.get_remote_exit_info",
            return_value=(error_state, error_message)
    ):

        # code logic to test
        distributor_service.refresh_status_from_db(TaskInstanceStatus.TRIAGING)
        distributor_service.process_status(TaskInstanceStatus.TRIAGING)

    # check the jobs to be UNKNOWN_ERROR as expected
    with app.app_context():
        sql = """
        SELECT ti.*
        FROM task_instance ti
        WHERE ti.task_id in :task_ids
        """
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[ti.task_id for ti in tis])
            .all()
        )
        DB.session.commit()

        assert len(res) == len(tis)

        for ti in res:
            assert ti.status == error_state
            assert ti.errors[0].description == error_message

    distributor.stop()
