import time

import pytest

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

    def is_alive(self):
        return True


def test_instantiate_queued_jobs(tool, db_cfg, client_env, task_template):
    """tests that a task can be instantiated and run and log done"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(
        arg="echo 1",
        cluster_name="sequential"
    )
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             SequentialDistributor(), requester=requester)
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.status
        FROM task_instance
        WHERE task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": t1.task_id}).fetchone()
        print(f"foo {res}")
        DB.session.commit()
    assert res[0] == "D"


def test_n_queued(tool, db_cfg, client_env, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.dummy import DummyDistributor
    from jobmon.serializers import SerializeTask
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)

    workflow = tool.create_workflow(name="test_n_queued")
    workflow.add_tasks(tasks)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             DummyDistributor(), requester=requester,
                                             n_queued=3)

    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    # comparing results and times of old query vs new query
    rc, response = workflow.requester.send_request(
        app_route=f'/distributor/workflow/{workflow.workflow_id}/queued_tasks/1000',
        message={},
        request_type='get')
    all_jobs = [SerializeTask.kwargs_from_wire(j) for j in response['task_dcts']]

    # now new query that should only return 3 jobs
    select_jobs = distributor_service._get_tasks_queued_for_instantiation()

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20


# @pytest.mark.parametrize('sge', [QsubAttribute.NO_DIST_ID,
#                                  QsubAttribute.UNPARSABLE])
# def test_no_distributor_id(db_cfg, client_env, monkeypatch, sge):
#     """test that things move successfully into 'W' state if the executor
#     returns the correct id"""
#     from jobmon.client.templates.unknown_workflow import UnknownWorkflow
#     from jobmon.client.api import BashTask
#     from jobmon.client.distributor.distributor_service import DistributorService
#     from jobmon.requester import Requester

#     t1 = BashTask("echo 2", executor_class="DummyExecutor")
#     workflow = UnknownWorkflow(f"my_simple_dag_{sge}",
#                                executor_class="DummyExecutor",
#                                seconds_until_timeout=1)
#     workflow.add_task(t1)
#     workflow.bind()
#     wfr = workflow._create_workflow_run()

#     requester = Requester(client_env)
#     distributor = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
#                                       workflow._executor, requester=requester)
#     with pytest.raises(RuntimeError):
#         wfr.execute_interruptible(MockDistributorProc(),
#                                   seconds_until_timeout=1)

#     def mock_execute(*args, **kwargs):
#         return sge
#     monkeypatch.setattr(distributor.executor, "execute", mock_execute)

#     distributor._get_tasks_queued_for_instantiation()
#     distributor.distribute()

#     # check the job finished
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     with app.app_context():
#         sql = """
#         SELECT task_instance.status
#         FROM task_instance
#         WHERE task_id = :task_id"""
#         res = DB.session.execute(sql, {"task_id": t1.task_id}).fetchone()
#         DB.session.commit()
#     assert res[0] == "W"


def test_concurrency_limiting(tool, db_cfg, client_env, task_template):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)
    workflow = tool.create_workflow(name="test_concurrency_limiting",
                                    max_concurrently_running=2)
    # TODO: parallelism=3
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             MultiprocessDistributor(parallelism=3),
                                             requester=requester)

    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
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

    distributor_service.distributor.stop(list(distributor_service._submitted_or_running.keys()))


def test_dynamic_concurrency_limiting(tool, db_cfg, client_env, task_template):
    """ tests that the CLI functionality to update concurrent jobs behaves as expected"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.client.status_commands import concurrency_limit
    from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}",
                                         compute_resources={"queue": "null.q", "cores": 1})
        tasks.append(task)

    workflow = tool.create_workflow(name="dynamic_concurrency_limiting",
                                    max_concurrently_running=2)
    # TODO: parallelism
    # workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    # queue the tasks
    swarm = SwarmWorkflowRun(workflow_id=wfr.workflow_id, workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()))
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    # Started with a default of 2. Adjust up to 5 and try again
    concurrency_limit(workflow.workflow_id, 5)

    # wfr2 = workflow._create_workflow_run(resume=True)

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             MultiprocessDistributor(parallelism=3),
                                             requester=requester)

    # Query should return 5 jobs
    select_tasks = distributor_service._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 5

    distributor_service.distributor.stop(list(distributor_service._submitted_or_running.keys()))
