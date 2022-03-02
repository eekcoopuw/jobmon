import time
from typing import Dict, Union

from unittest import mock
from jobmon.requester import Requester
from jobmon.constants import TaskInstanceStatus


def test_launched_processor_on_multiprocess(tool, db_cfg, client_env, task_template):
    """tests that a task can be instantiated and run and log error"""
    from datetime import datetime
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor

    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )

    t1 = task_template.create_task(arg="echo 1", cluster_name="multiprocess")
    t2 = task_template.create_task(arg="echo 2", cluster_name="multiprocess")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_multiprocess")

    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor_service = DistributorService(
        MultiprocessDistributor(),
        requester=requester,
        raise_on_error=True
    )

    distributor_service.set_workflow_run(wfr.workflow_run_id)

    distributor_service.process_status("Q")

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
        print(f"foo {res}")
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "I"
    assert res[1][1] == "I"

    # Queued status should have turned into Instantiated status as well.
    assert \
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.QUEUED]) == 0
    assert \
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.INSTANTIATED]) \
        == 2
    assert \
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED]) \
        == 0

    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check the job to be Launched
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        print(f"foo {res}")
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "O"
    assert res[1][1] == "O"

    # check the report_by_date based on time_base
    time_base = datetime.now()

    distributor_service.log_task_instance_report_by_date()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, task_instance.status, report_by_date
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        print(f"foo {res}")
        DB.session.commit()

    assert len(res) == 2
    assert res[0][2] > time_base
    assert res[1][2] > time_base

    # turn the 2 task instances from LAUNCHED to UNKOWN_ERROR

    # stage the report_by_date to be 1 hour past
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE task_instance
        SET report_by_date = CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        WHERE task_id in :task_ids"""
        DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]})
        DB.session.commit()

    mocked_return_value: Dict[Union[int, str], str] = {}
    for task_instance in distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED]:
        mocked_return_value[task_instance.array_batch.distributor_id] = "Mocked Error"

    with mock.patch(
        "jobmon.cluster_type.multiprocess.multiproc_distributor."
        "MultiprocessDistributor.get_array_queueing_errors",
            return_value=mocked_return_value
    ) :
        # code logic to test
        distributor_service.process_status(TaskInstanceStatus.LAUNCHED)

        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            sql = """
            SELECT id, task_instance.status, report_by_date
            FROM task_instance
            WHERE task_id in :task_ids
            ORDER BY id"""
            res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
            DB.session.commit()

        assert len(res) == 2
        assert res[0][1] == TaskInstanceStatus.UNKNOWN_ERROR
        assert res[1][1] == TaskInstanceStatus.UNKNOWN_ERROR
