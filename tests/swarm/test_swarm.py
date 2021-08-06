import logging

from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.constants import WorkflowRunStatus
from jobmon.requester import Requester
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus

import pytest

from jobmon.client.tool import Tool


logger = logging.getLogger(__name__)


class MockDistributorProc:

    def is_alive(self):
        return True


def test_blocking_update_timeout(tool, task_template):
    """This test runs a 1 task workflow and confirms that the workflow_run
    will timeout with an appropriate error message if timeout is set
    """

    task = task_template.create_task(arg="sleep 3", name="foobarbaz")
    workflow = tool.create_workflow(name="my_simple_dag")
    workflow.add_tasks([task])
    workflow.bind()
    workflow._distributor_proc = MockDistributorProc()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(workflow_id=workflow.workflow_id,
                             workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()),
                             requester=workflow.requester)

    with pytest.raises(RuntimeError) as error:
        workflow._run_swarm(swarm, seconds_until_timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sync_statuses(client_env, tool, task_template):
    """this test executes a single task workflow where the task fails. It
    is testing to confirm that the status updates are propagated into the
    swarm objects"""

    # client calls
    task = task_template.create_task(arg="fizzbuzz", name="bar", max_attempts=1)
    workflow = tool.create_workflow(name="my_simple_dag")
    workflow.add_tasks([task])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # move workflow to launched state
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             SequentialDistributor(),
                                             requester=workflow.requester)

    # swarm calls
    swarm = SwarmWorkflowRun(workflow_id=workflow.workflow_id,
                             workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()),
                             requester=workflow.requester)
    swarm.update_status(WorkflowRunStatus.RUNNING)
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())

    # test initial dag state updates last_sync
    now = swarm.last_sync
    assert now is not None

    # distribute the task
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    swarm.block_until_newly_ready_or_all_done()

    new_now = swarm.last_sync
    assert new_now > now
    assert len(swarm.all_error) > 0


@pytest.mark.skip(reason="need executor plugin interface")
def test_wedged_dag(monkeypatch, client_env, db_cfg, task_template):
    """This test runs a 3 task dag where one of the tasks updates it status
    without updating its status date. This would cause the normal pathway of
    status collection in the workflow run to fail. Instead the test uses the
    wedged_workflow_sync_interval set to 1 second to force a full sync of
    the workflow tasks which resolves the wedge"""
    from jobmon.client.distributor.strategies import dummy
    from jobmon.client.distributor.worker_node.execution_wrapper \
        import parse_arguments
    from jobmon.client.api import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.distributor.distributor_service import DistributorService

    class MockDummyExecutor(dummy.DummyExecutor):

        wedged_task_id = None
        app = db_cfg["app"]
        DB = db_cfg["DB"]

        def execute(self, command: str, name: str, executor_parameters) -> int:
            logger.info("Now entering MockDummy execute")
            kwargs = parse_arguments(command)

            # need to get task id from task instance here to compare to wedged
            # task id that will be set later in the code
            with self.app.app_context():
                task_instance = self.DB.session.query(TaskInstance).filter_by(
                    id=kwargs["task_instance_id"]).one()
                task_id = int(task_instance.task.id)

            if task_id == self.wedged_task_id:
                logger.info(f"task instance is {self.wedged_task_id}, entering"
                            " first if statement")
                task_inst_query = """
                    UPDATE task_instance
                    SET status = 'D'
                    WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])
                task_query = """
                    UPDATE task
                    SET task.status = 'D',
                        task.status_date = SUBTIME(CURRENT_TIMESTAMP(),
                                                   SEC_TO_TIME(600))
                    WHERE task.id = {task_id}
                """.format(task_id=task_id)
            else:
                logger.info(f"task instance is not {self.wedged_task_id}, "
                            "entering else branch")
                task_inst_query = """
                    UPDATE task_instance
                    SET status = 'D',
                        status_date = CURRENT_TIMESTAMP()
                    WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])
                task_query = """
                    UPDATE task
                    SET task.status = 'D',
                        task.status_date = CURRENT_TIMESTAMP()
                    WHERE task.id = {task_id}
                """.format(task_id=task_id)

            with self.app.app_context():
                self.DB.session.execute(task_inst_query)
                self.DB.session.commit()
                self.DB.session.execute(task_query)
                self.DB.session.commit()
            return super().execute(command, name, executor_parameters)

    t1 = BashTask("sleep 3", executor_class="DummyExecutor",
                  max_runtime_seconds=1)
    t2 = BashTask("sleep 5", executor_class="DummyExecutor",
                  max_runtime_seconds=1)
    t3 = BashTask("sleep 7", executor_class="DummyExecutor",
                  upstream_tasks=[t2], max_runtime_seconds=1)

    workflow = UnknownWorkflow(executor_class="DummyExecutor",
                               seconds_until_timeout=300)
    workflow.add_tasks([t1, t2, t3])

    # bind workflow to db
    workflow._bind()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr.update_status(WorkflowRunStatus.INSTANTIATING)
    wfr.update_status(WorkflowRunStatus.LAUNCHED)

    # queue task 1
    for task in [t1, t2]:
        swarm_task = wfr.swarm_tasks[task.task_id]
        wfr._adjust_resources_and_queue(swarm_task)

    # run initial sync
    wfr._distributor_proc = MockDistributorProc()
    with pytest.raises(RuntimeError):
        wfr._execute(seconds_until_timeout=1)
    assert wfr.swarm_tasks[t1.task_id].status == \
        TaskStatus.QUEUED_FOR_INSTANTIATION
    assert wfr.swarm_tasks[t2.task_id].status == \
        TaskStatus.QUEUED_FOR_INSTANTIATION
    assert wfr.swarm_tasks[t3.task_id].status == \
        TaskStatus.REGISTERED

    # launch task on executor
    execute = MockDummyExecutor()
    execute.wedged_task_id = t2.task_id
    requester = Requester(client_env)
    distributor = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                     workflow._executor, requester=requester)
    distributor.executor.start()
    distributor.distribute()

    # run the normal workflow sync protocol. only t1 should be done
    with pytest.raises(RuntimeError):
        wfr._execute(seconds_until_timeout=1)
    assert wfr.swarm_tasks[t1.task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[t2.task_id].status == \
        TaskStatus.QUEUED_FOR_INSTANTIATION
    assert wfr.swarm_tasks[t3.task_id].status == TaskStatus.REGISTERED

    # now run wedged dag route. make sure task 2 is now in done state
    with pytest.raises(RuntimeError):
        wfr._execute(seconds_until_timeout=1,
                     wedged_workflow_sync_interval=-1)
    assert wfr.swarm_tasks[t1.task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[t2.task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[t3.task_id].status == \
        TaskStatus.QUEUED_FOR_INSTANTIATION

    # distribute the third task
    distributor.distribute()

    # confirm that the final task finishes appropiately
    completed, _ = wfr._block_until_any_done_or_error(timeout=1)
    assert list(completed)[0].task_id == t3.task_id


def test_fail_fast(tool, task_template):
    """set up a dag where a middle job fails. The fail_fast parameter should
    ensure that not all tasks finish"""

    # The sleep for t3 must be long so that the swarm has time to notice that t2
    # died and react accordingly.
    workflow = tool.create_workflow(name="test_fail_fast")
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="erroring_out 1", upstream_tasks=[t1])
    t3 = task_template.create_task(arg="sleep 20", upstream_tasks=[t1])
    t4 = task_template.create_task(arg="sleep 3", upstream_tasks=[t3])
    t5 = task_template.create_task(arg="sleep 4", upstream_tasks=[t4])

    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.bind()

    with pytest.raises(RuntimeError):
        workflow.run(fail_fast=True)

    assert len(workflow.task_errors) == 1
    num_done = len([task for task in workflow.tasks.values()
                    if task.final_status == TaskStatus.DONE])
    assert num_done >= 1
    assert num_done <= 3


def test_propagate_result(tool, task_template):
    """set up workflow with 3 tasks on one layer and 3 tasks as dependant"""

    workflow = tool.create_workflow(name="test_propagate_result")

    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")
    t4 = task_template.create_task(arg="echo 4", upstream_tasks=[t1, t2, t3])
    t5 = task_template.create_task(arg="echo 5", upstream_tasks=[t1, t2, t3])
    t6 = task_template.create_task(arg="echo 6", upstream_tasks=[t1, t2, t3])
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # run the distributor
    workflow._distributor_proc = workflow._start_distributor_service(wfr.workflow_run_id)

    # swarm calls
    swarm = SwarmWorkflowRun(workflow_id=workflow.workflow_id,
                             workflow_run_id=wfr.workflow_run_id,
                             tasks=list(workflow.tasks.values()),
                             requester=workflow.requester)
    workflow._run_swarm(swarm)

    # stop the subprocess
    workflow._distributor_stop_event.set()

    assert swarm.status == WorkflowRunStatus.DONE
    assert len(swarm.all_done) == 6
    assert swarm.swarm_tasks[t4.task_id].num_upstreams_done >= 3
    assert swarm.swarm_tasks[t5.task_id].num_upstreams_done >= 3
    assert swarm.swarm_tasks[t6.task_id].num_upstreams_done >= 3
