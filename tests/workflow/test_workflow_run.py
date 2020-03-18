import pytest
import time

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.scheduler.execution_config import \
    ExecutionConfig
from jobmon.models.task_status import TaskStatus

logger = logging.getLogger(__name__)


class MockSchedulerProc:

    def is_alive(self):
        return True


def test_blocking_update_timeout(client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    task = BashTask("sleep 3", executor_class="SequentialExecutor",
                    name="foobarbaz")
    workflow = UnknownWorkflow("my_simple_dag",
                               executor_class="SequentialExecutor")
    workflow.add_tasks([task])
    workflow._bind()
    wfr = workflow._create_workflow_run()

    with pytest.raises(RuntimeError) as error:
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sync(client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    task = BashTask(command="fizzbuzz", name="bar", max_attempts=1)

    workflow = UnknownWorkflow("my_simple_dag",
                               executor_class="SequentialExecutor")
    workflow.add_tasks([task])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    now = wfr.last_sync
    assert now is not None

    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)

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
    from jobmon.models.workflow_run_status import WorkflowRunStatus
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    class MockDummyExecutor(dummy.DummyExecutor):

        wedged_task_id = None

        def execute(self, command: str, name: str, executor_parameters) -> int:
            logger.warning("Now entering MockDummy execute")
            kwargs = parse_arguments(command)
            if kwargs["task_instance_id"] == self.wedged_task_id:
                logger.info("task instance is 1, entering first if statement")
                task_inst_query = """
                UPDATE task_instance
                SET status = 'D'
                WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])
                task_query = """
                UPDATE task
                JOIN task_instance ON task.id = task_instance.id
                SET task.status = 'D',
                    task.status_date = SUBTIME(UTC_TIMESTAMP(),
                                               SEC_TO_TIME(600))
                WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])
            else:
                logger.warning("task instance is not 1, entering else statement")
                task_inst_query = """
                UPDATE task_instance
                SET status = 'D',
                    status_date = UTC_TIMESTAMP()
                WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])
                task_query = """
                UPDATE task
                JOIN task_instance ON task.id = task_instance.id
                SET task.status = 'D',
                    task.status_date = UTC_TIMESTAMP()
                WHERE task_instance.id = {task_instance_id}
                """.format(task_instance_id=kwargs["task_instance_id"])

            app = db_cfg["app"]
            DB = db_cfg["DB"]
            with app.app_context():
                DB.session.execute(task_inst_query)
                DB.session.commit()
                DB.session.execute(task_query)
                DB.session.commit()
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
    cfg = ExecutionConfig.from_defaults()
    execute = MockDummyExecutor()
    execute.wedged_task_id = t2.task_id
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id, execute, cfg)
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

    # schedule the second task
    # swarm_task = wfr.swarm_tasks[t3.task_id]
    # wfr._adjust_resources_and_queue(swarm_task)
    scheduler.schedule()

    # confirm that the dag finishes appropiately
    wfr._execute(seconds_until_timeout=1)

    assert wfr.status == WorkflowRunStatus.DONE
