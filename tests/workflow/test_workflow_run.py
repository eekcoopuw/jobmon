import pytest
import logging
import time

from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.requester import Requester

logger = logging.getLogger(__name__)


class MockSchedulerProc:

    def is_alive(self):
        return True


def test_blocking_update_timeout(client_env):
    """This test runs a 1 task workflow and confirms that the workflow_run
    execution will timeout with an appropriate error message if timeout is set
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    task = BashTask("sleep 3", executor_class="SequentialExecutor",
                    name="foobarbaz")
    workflow = UnknownWorkflow("my_simple_dag",
                               executor_class="SequentialExecutor")
    workflow.add_tasks([task])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    with pytest.raises(RuntimeError) as error:
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sync(client_env):
    """this test executes a single task workflow where the task fails. It
    is testing to confirm that the status updates are propagated into the
    swarm objects"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    task = BashTask(command="fizzbuzz", name="bar", max_attempts=1)

    workflow = UnknownWorkflow("my_simple_dag",
                               executor_class="SequentialExecutor")
    workflow.add_tasks([task])
    workflow.bind()
    wfr = workflow._create_workflow_run()
    now = wfr.last_sync
    assert now is not None

    requester = Requester(client_env)
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)

    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=2)

    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

    time.sleep(1)
    wfr._parse_adjusting_done_and_errors(wfr._task_status_updates())

    new_now = wfr.last_sync
    assert new_now > now
    assert len(wfr.all_error) > 0


@pytest.mark.skip(reason="need executor plugin interface")
def test_wedged_dag(monkeypatch, client_env, db_cfg):
    """This test runs a 3 task dag where one of the tasks updates it status
    without updating its status date. This would cause the normal pathway of
    status collection in the workflow run to fail. Instead the test uses the
    wedged_workflow_sync_interval set to 1 second to force a full sync of
    the workflow tasks which resolves the wedge"""
    from jobmon.client.execution.strategies import dummy
    from jobmon.client.execution.worker_node.execution_wrapper \
        import parse_arguments
    from jobmon.client.api import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    class MockDummyExecutor(dummy.DummyExecutor):

        wedged_task_id = None
        app = db_cfg["app"]
        DB = db_cfg["DB"]

        def execute(self, command: str, name: str, executor_parameters) -> int:
            logger.warning("Now entering MockDummy execute")
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

    # queue task 1
    for task in [t1, t2]:
        swarm_task = wfr.swarm_tasks[task.task_id]
        wfr._adjust_resources_and_queue(swarm_task)

    # run initial sync
    wfr._scheduler_proc = MockSchedulerProc()
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
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
    scheduler.executor.start()
    scheduler.schedule()

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

    # schedule the third task
    scheduler.schedule()

    # confirm that the final task finishes appropiately
    completed, _ = wfr._block_until_any_done_or_error(timeout=1)
    assert list(completed)[0].task_id == t3.task_id


def test_fail_fast(client_env):
    """set up a dag where a middle job fails. The fail_fast parameter should
    ensure that not all tasks finish"""
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # The sleep for t3 must be long so that the executor has time to notice that t2
    # died and react accordingly.
    unknown_tool = Tool()
    workflow = unknown_tool.create_workflow(name="test_fail_fast")
    t1 = BashTask("sleep 1", executor_class="SequentialExecutor")
    t2 = BashTask("erroring_out 1", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    t3 = BashTask("sleep 20", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    t4 = BashTask("sleep 3", upstream_tasks=[t3],
                  executor_class="SequentialExecutor")
    t5 = BashTask("sleep 4", upstream_tasks=[t4],
                  executor_class="SequentialExecutor")

    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.set_executor(SequentialExecutor())
    wfr = workflow.run(fail_fast=True)

    assert len(wfr.all_error) == 1
    assert len(wfr.all_done) >= 1
    assert len(wfr.all_done) <= 3


def test_propagate_result(client_env):
    """set up workflow with 3 tasks on one layer and 3 tasks as dependant"""
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    unknown_tool = Tool()
    workflow = unknown_tool.create_workflow(name="test_propagate_result")

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    t2 = BashTask("echo 2", executor_class="SequentialExecutor")
    t3 = BashTask("echo 3", executor_class="SequentialExecutor")
    t4 = BashTask("echo 4", upstream_tasks=[t1, t2, t3],
                  executor_class="SequentialExecutor")
    t5 = BashTask("echo 5", upstream_tasks=[t1, t2, t3],
                  executor_class="SequentialExecutor")
    t6 = BashTask("echo 6", upstream_tasks=[t1, t2, t3],
                  executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.set_executor(SequentialExecutor())
    wfr = workflow.run(seconds_until_timeout=300)

    assert len(wfr.all_done) == 6
    keys = list(wfr.swarm_tasks.keys())
    assert wfr.swarm_tasks[keys[3]].num_upstreams_done >= 3
    assert wfr.swarm_tasks[keys[4]].num_upstreams_done >= 3
    assert wfr.swarm_tasks[keys[5]].num_upstreams_done >= 3
