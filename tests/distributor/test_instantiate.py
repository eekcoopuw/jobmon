import time


from unittest import mock
from jobmon.requester import Requester
from jobmon.serializers import SerializeDistributorTask
from jobmon.constants import TaskInstanceStatus


class MockDistributorProc:
    def is_alive(self):
        return True


def test_instantiate_queued_tasks_on_sequential(tool, db_cfg, client_env, task_template):
    """tests that a task can be instantiated and run and log done"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    t2 = task_template.create_task(arg="echo 2", cluster_name="sequential")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_sequential")

    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor_service = DistributorService(
        SequentialDistributor(),
        requester=requester,
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
        res = DB.session.execute(sql, {"task_ids": [t1.task_id, t2.task_id]}).all()
        print(f"foo {res}")
        DB.session.commit()

    assert len(res) == 2
    assert res[0][1] == "D"
    assert res[1][1] == "D"


def test_instantiate_queued_tasks_on_multiprocess(tool, db_cfg, client_env, task_template):
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


def test_n_queued(tool, db_cfg, client_env, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.dummy import DummyDistributor

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)

    workflow = tool.create_workflow(name="test_n_queued")
    workflow.add_tasks(tasks)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        DummyDistributor(),
        requester=requester,
        n_queued=3,
    )

    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    # comparing results and times of old query vs new query
    rc, response = workflow.requester.send_request(
        app_route=f"/workflow/{workflow.workflow_id}/queued_tasks/1000",
        message={},
        request_type="get",
    )
    all_jobs = [
        SerializeDistributorTask.kwargs_from_wire(j) for j in response["task_dcts"]
    ]

    # now new query that should only return 3 jobs
    select_jobs = distributor_service._get_tasks_queued_for_instantiation()

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20


def test_submit_raises_error(db_cfg, tool):
    """test that things move successfully into 'W' state if the executor
    returns the correct id"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor

    class ErrorDistributor(SequentialDistributor):
        def submit_to_batch_distributor(
            self, command: str, name: str, requested_resources
        ) -> int:
            raise ValueError("No distributor_id")

    workflow = tool.create_workflow(name=f"test_submit_raises_error")
    task1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 120")
    workflow.add_task(task1)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())

    distributor = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        ErrorDistributor(),
        requester=workflow.requester,
    )

    distributor._get_tasks_queued_for_instantiation()
    distributor.distribute()

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


def test_concurrency_limiting(tool, db_cfg, client_env, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)
    workflow = tool.create_workflow(
        name="test_concurrency_limiting", max_concurrently_running=2
    )
    # TODO: parallelism=3
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        MultiprocessDistributor(parallelism=3),
        requester=requester,
    )

    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    # now new query that should only return 2 jobs
    select_tasks = distributor_service._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 2
    for task in select_tasks:
        distributor_service._create_task_instance(task)

    # jobs are now in I state. should return -
    select_tasks = distributor_service._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 0

    # start executor and wait for tasks to move to running
    distributor_service.distributor.start()
    while not distributor_service.distributor.task_queue.empty():
        time.sleep(1)

    # should return 0 still because tasks are running
    select_tasks = distributor_service._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 0

    distributor_service.distributor.stop(
        list(distributor_service._submitted_or_running.keys())
    )


def test_dynamic_concurrency_limiting(tool, db_cfg, client_env, task_template):
    """tests that the CLI functionality to update concurrent jobs behaves as expected"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.client.status_commands import concurrency_limit
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = task_template.create_task(
            arg=f"sleep {i}", compute_resources={"queue": "null.q", "cores": 1}
        )
        tasks.append(task)

    workflow = tool.create_workflow(
        name="dynamic_concurrency_limiting", max_concurrently_running=2
    )
    # TODO: parallelism
    # workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    # queue the tasks
    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    # Started with a default of 2. Adjust up to 5 and try again
    concurrency_limit(workflow.workflow_id, 5)

    # wfr2 = workflow._create_workflow_run(resume=True)

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        MultiprocessDistributor(parallelism=3),
        requester=requester,
    )

    # Query should return 5 jobs
    select_tasks = distributor_service._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 5

    distributor_service.distributor.stop(
        list(distributor_service._submitted_or_running.keys())
    )
