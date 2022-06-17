from datetime import datetime, timedelta
from typing import Dict

import pytest
from sqlalchemy.sql import text

from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.status_commands import concurrency_limit
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.builtins.multiprocess.multiproc_distributor import (
    MultiprocessDistributor,
)
from jobmon.builtins.sequential.seq_distributor import SequentialDistributor
from jobmon.constants import TaskInstanceStatus


def test_instantiate_job(tool, db_cfg, client_env, task_template):
    """tests that a task can be instantiated and run and log done"""
    from jobmon.server.web.models.task_instance import TaskInstance

    # create the workflow and bind to database
    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    t2 = task_template.create_task(arg="echo 2", cluster_name="sequential")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_sequential")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        SequentialDistributor("sequential"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
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
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    assert len(res) == 2
    assert res[0].status == "I"
    assert res[1].status == "I"

    # Queued status should have turned into Instantiated status as well.
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.QUEUED])
        == 0
    )
    assert (
        len(
            distributor_service._task_instance_status_map[
                TaskInstanceStatus.INSTANTIATED
            ]
        )
        == 2
    )
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 0
    )

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # Once processed from INSTANTIATED, the sequential (being a single process), would
    # carry it all the way through to D
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    assert len(res) == 2
    assert res[0].status == "D"
    assert res[1].status == "D"


def test_instantiate_array(tool, db_cfg, client_env, task_template):
    """tests that a task can be instantiated and run and log error"""
    from jobmon.server.web.models.task_instance import TaskInstance

    # create the workflow and bind to database
    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )
    t1 = task_template.create_task(arg="echo 1", cluster_name="multiprocess")
    t2 = task_template.create_task(arg="echo 2", cluster_name="multiprocess")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_multiprocess")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
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
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    assert len(res) == 2
    assert res[0].status == "I"
    assert res[1].status == "I"

    # Queued status should have turned into Instantiated status as well.
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.QUEUED])
        == 0
    )
    assert (
        len(
            distributor_service._task_instance_status_map[
                TaskInstanceStatus.INSTANTIATED
            ]
        )
        == 2
    )
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 0
    )

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check the job to be Launched
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, status, distributor_id, array_step_id
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    assert len(res) == 2
    assert res[0].status == "O"
    assert res[1].status == "O"
    assert res[0].distributor_id is not None
    assert res[1].distributor_id is not None

    # Check that distributor id is logged correctly
    submitted_job_id = distributor_service.cluster_interface._next_job_id - 1
    expected_dist_id = distributor_service.cluster_interface._get_subtask_id
    assert res[0].distributor_id == expected_dist_id(
        submitted_job_id, res[0].array_step_id
    )
    assert res[1].distributor_id == expected_dist_id(
        submitted_job_id, res[1].array_step_id
    )


def test_job_submit_raises_error(db_cfg, tool):
    """test that things move successfully into 'W' state if the executor
    returns the correct id"""

    class ErrorDistributor(SequentialDistributor):
        def submit_to_batch_distributor(
            self, command: str, name: str, requested_resources
        ) -> str:
            raise ValueError("No distributor_id")

    workflow = tool.create_workflow(name="test_submit_raises_error")
    task1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 120")
    workflow.add_task(task1)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        ErrorDistributor("sequential"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.status
        FROM task_instance
        WHERE task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task1.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "W"


def test_array_submit_raises_error(db_cfg, tool):
    """test that things move successfully into 'W' state if the executor
    returns the correct id"""
    from jobmon.server.web.models.task_instance import TaskInstance

    class ErrorDistributor(MultiprocessDistributor):
        def submit_array_to_batch_distributor(
            self, command: str, name: str, requested_resources, array_length: int
        ) -> Dict[int, str]:
            raise ValueError("No distributor_id")

    # create the workflow and bind to database
    tool.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources={"queue": "null.q"}
    )
    t1 = tool.active_task_templates["simple_template"].create_task(arg="echo 1")
    t2 = tool.active_task_templates["simple_template"].create_task(arg="echo 2")
    workflow = tool.create_workflow(name="test_array_submit_raises_error")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        ErrorDistributor("sequential"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT id, task_instance.status
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        res = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    for task in res:
        assert task.status == "W"


def test_task_template_concurrency_limiting(tool, db_cfg, client_env):
    """Test that we are able to set concurrency limit on TaskTemplate level.

    Create 20 sleep tasks associated with the tasktemplate, set the limit to 2 and make sure
    that only 2 tasks launch.
    """
    task_template = tool.get_task_template(
        template_name="concurrency_limit_task_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)
    workflow = tool.create_workflow(
        name="test_task_template_concurrency_limiting"
    )

    workflow.add_tasks(tasks)
    workflow.set_task_template_max_concurrency_limit(task_template_name=task_template.template_name,
                                                     limit=2)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=3),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 2
    )

    distributor_service.cluster_interface.stop()


def test_workflow_concurrency_limiting(tool, db_cfg, client_env, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)
    workflow = tool.create_workflow(
        name="test_concurrency_limiting", max_concurrently_running=2
    )
    workflow.add_tasks(tasks)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=3),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 2
    )

    distributor_service.cluster_interface.stop()


@pytest.mark.parametrize(
    "wf_limit, array_limit, expected_len",
    [(10_000, 2, 2), (2, 10_000, 2), (2, 3, 2), (3, 2, 2)],
)
def test_array_concurrency(
    tool, db_cfg, client_env, array_template, wf_limit, array_limit, expected_len
):
    """Use Case 1: Array concurrency limit is set, workflow is not. Array should be limited by
    the array's max_concurrently running value"""
    # Use Case 1: Array concurrency limit is set, workflow concurrency limit not set
    tasks1 = array_template.create_tasks(
        arg=[1, 2, 3],
        cluster_name="multiprocess",
        compute_resources={"queue": "null.q"},
        max_concurrently_running=array_limit,
    )

    workflow = tool.create_workflow(
        name="test_array_concurrency_1", max_concurrently_running=wf_limit
    )
    workflow.add_tasks(tasks1)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=3),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == expected_len
    )

    distributor_service.cluster_interface.stop()


def test_dynamic_concurrency_limiting(tool, db_cfg, task_template):
    """tests that the CLI functionality to update concurrent jobs behaves as expected"""

    tasks = []
    for i in range(20):
        task = task_template.create_task(
            arg=f"sleep {i}", compute_resources={"queue": "null.q", "cores": 1}
        )
        tasks.append(task)

    workflow = tool.create_workflow(
        name="test_dynamic_concurrency_limiting", max_concurrently_running=2
    )

    workflow.add_tasks(tasks)
    workflow.bind()

    # Start with limit of 2. Adjust up to 5 and try again

    wfr = workflow._create_workflow_run()
    # queue the tasks
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 2
    )

    concurrency_limit(workflow.workflow_id, 5)
    # This checks the route on the server
    swarm._synchronize_max_concurrently_running()
    swarm.process_commands()
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)
    assert (
        len(distributor_service._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        == 5
    )


def test_array_launch_transition(db_cfg, web_server_in_memory):

    from jobmon.server.web.models.task import Task
    from jobmon.server.web.models.task_instance import TaskInstance
    from jobmon.constants import TaskStatus, TaskInstanceStatus

    # Make up some tasks and task instances in I state
    app, db = db_cfg["app"], db_cfg["DB"]
    t = Task(
        array_id=1,
        task_args_hash=123,
        command="echo 1",
        status=TaskStatus.INSTANTIATING,
    )

    # Add the task
    with app.app_context():
        db.session.add(t)
        db.session.commit()

    ti_params = {
        "task_id": t.id,
        "status": TaskInstanceStatus.INSTANTIATED,
        "array_id": 1,
        "array_batch_num": 1,
        "array_step_id": 0,
    }

    ti1 = TaskInstance(**ti_params)
    ti2 = TaskInstance(**dict(ti_params, array_step_id=1))
    ti3 = TaskInstance(**dict(ti_params, array_step_id=2))

    # add tis to db
    with app.app_context():
        db.session.add_all([ti1, ti2, ti3])
        db.session.commit()

    # Post the transition route, check what comes back
    resp = web_server_in_memory.post(
        "/array/1/transition_to_launched",
        json={
            "batch_number": 1,
            "next_report_increment": 5 * 60,  # 5 minutes to report
        },
    )
    assert resp.status_code == 200

    # Check the statuses are updated
    with app.app_context():
        tnew = db.session.query(Task).where(Task.id == t.id).one()
        ti1_r, ti2_r, ti3_r = (
            db.session.query(TaskInstance)
            .where(TaskInstance.id.in_([ti1.id, ti2.id, ti3.id]))
            .all()
        )

        assert tnew.status == TaskStatus.LAUNCHED
        assert [ti1_r.status, ti2_r.status, ti3_r.status] == [
            TaskInstanceStatus.LAUNCHED
        ] * 3

        # Check a single datetime
        submitted_date = ti1_r.submitted_date
        next_update_date = ti1_r.report_by_date
        assert next_update_date > datetime.now()
        assert next_update_date <= timedelta(minutes=5) + datetime.now()
        assert datetime.now() - timedelta(minutes=5) < submitted_date < datetime.now()

    # Post a request to log the distributor ids
    resp = web_server_in_memory.post(
        "/array/1/log_distributor_id",
        json={
            "array_batch_num": 1,
            "distributor_id_map": {
                "0": ("123_1", "foo/out/file", "foo/err/file"),
                "1": ("123_2", "foo/out/file", "foo/err/file"),
                "2": ("123_3", "foo/out/file", "foo/err/file"),
            },
        },
    )
    assert resp.status_code == 200

    with app.app_context():
        ti1_r, ti2_r, ti3_r = (
            db.session.query(TaskInstance)
            .where(TaskInstance.id.in_([ti1.id, ti2.id, ti3.id]))
            .all()
        )

        assert [ti1_r.distributor_id, ti2_r.distributor_id, ti3_r.distributor_id] == [
            "123_1",
            "123_2",
            "123_3",
        ]

        assert {ti1_r.stdout, ti2_r.stdout, ti3_r.stdout} == {"foo/out/file"}
        assert {ti1_r.stderr, ti2_r.stderr, ti3_r.stderr} == {"foo/err/file"}
