import time

from jobmon.constants import WorkflowRunStatus

import pytest


class MockDistributorProc:

    def is_alive(self):
        return True


def test_instantiate_queued_jobs(db_cfg, client_env):
    """tests that a task can be instantiated and run and log done"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.requester import Requester

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = UnknownWorkflow("test_instantiate_queued_jobs",
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=1)
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(),
                                  seconds_until_timeout=1)

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
        res = DB.session.execute(sql, {"task_id": t1.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "D"


def test_n_queued(db_cfg, client_env):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.serializers import SerializeTask
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)

    workflow = UnknownWorkflow("test_n_queued", seconds_until_timeout=1,
                               executor_class="DummyExecutor")
    workflow.set_executor(executor_class="DummyExecutor")
    workflow.add_tasks(tasks)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester,
                                      n_queued=3)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(),
                                  seconds_until_timeout=1)

    # comparing results and times of old query vs new query
    rc, response = workflow.requester.send_request(
        app_route=f'/distributor/workflow/{workflow.workflow_id}/queued_tasks/1000',
        message={},
        request_type='get')
    all_jobs = [
        SerializeTask.kwargs_from_wire(j)
        for j in response['task_dcts']]

    # now new query that should only return 3 jobs
    select_jobs = distributor._get_tasks_queued_for_instantiation()

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


def test_concurrency_limiting(db_cfg, client_env):
    """tests that we only return a subset of queued jobs based on the n_queued
    parameter"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.api import Tool, BashTask
    from jobmon.client.distributor.strategies.multiprocess import MultiprocessExecutor
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)
    unknown_tool = Tool()
    workflow = unknown_tool.create_workflow(name="test_concurrency_limiting",
                                            max_concurrently_running=2)
    workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)

    # now new query that should only return 2 jobs
    select_tasks = distributor._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 2
    for task in select_tasks:
        distributor._create_task_instance(task)

    # jobs are now in I state. should return -
    select_tasks = distributor._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 0

    # start executor and wait for tasks to move to running
    distributor.executor.start()
    while not distributor.executor.task_queue.empty():
        time.sleep(1)

    # should return 0 still because tasks are running
    select_tasks = distributor._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 0

    distributor.executor.stop(list(distributor._submitted_or_running.keys()))


def test_dynamic_concurrency_limiting(db_cfg, client_env):
    """ tests that the CLI functionality to update concurrent jobs behaves as expected"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.api import Tool, BashTask
    from jobmon.client.distributor.strategies.multiprocess import MultiprocessExecutor
    from jobmon.client.status_commands import concurrency_limit
    from jobmon.requester import Requester

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)
    unknown_tool = Tool()
    workflow = unknown_tool.create_workflow(name="dynamic_concurrency_limiting",
                                            max_concurrently_running=2)
    workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow.add_tasks(tasks)

    workflow.bind()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr.update_status(WorkflowRunStatus.INSTANTIATING)
    wfr.update_status(WorkflowRunStatus.LAUNCHED)

    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)

    wfr.update_status(WorkflowRunStatus.ERROR)

    # Started with a default of 2. Adjust up to 5 and try again
    concurrency_limit(workflow.workflow_id, 5)

    wfr2 = workflow._create_workflow_run(resume=True)

    requester = Requester(client_env)
    distributor = DistributorService(workflow.workflow_id, wfr2.workflow_run_id,
                                      workflow._executor, requester=requester)
    with pytest.raises(RuntimeError):
        wfr2.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)

    # Query should return 5 jobs
    select_tasks = distributor._get_tasks_queued_for_instantiation()
    assert len(select_tasks) == 5

    distributor.executor.stop(list(distributor._submitted_or_running.keys()))
