import time
from datetime import datetime, timedelta
from unittest.mock import patch

from jobmon.requester import Requester
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus

import pytest


@pytest.mark.parametrize("ti_state", [TaskInstanceStatus.UNKNOWN_ERROR,
                                      TaskInstanceStatus.KILL_SELF])
def test_ti_kill_self_state(db_cfg, client_env, ti_state):
    """should try to log a report by date after being set to the U or K state
    and fail"""
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.client.execution.scheduler.scheduler_config import SchedulerConfig

    tool = Tool()
    workflow = tool.create_workflow(name=f"test_ti_kill_self_state_{ti_state}")
    executor = MultiprocessExecutor(parallelism=1)
    workflow.set_executor(executor)
    task_a = BashTask("sleep 120", executor_class="MultiprocessExecutor")
    workflow.add_task(task_a)

    # bind workflow to db
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # queue task
    swarm_task = wfr.swarm_tasks[task_a.task_id]
    wfr._adjust_resources_and_queue(swarm_task)

    # launch task on executor
    requester = Requester(client_env)
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
    scheduler.executor.start()
    scheduler.schedule()

    # wait till task is running
    while not swarm_task.status == TaskInstanceStatus.RUNNING:
        time.sleep(1)
        tasks = wfr._task_status_updates()
        if tasks:
            swarm_task = tasks[0]

    # set task to kill self state. next heartbeat will fail and cause death
    scheduler_config = SchedulerConfig.from_defaults()
    max_heartbeat = datetime.utcnow() + timedelta(
        seconds=(scheduler_config.task_heartbeat_interval * scheduler_config.report_by_buffer))
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE task_instance
            SET status = '{}'
            WHERE task_instance.task_id = {}
            """.format(ti_state, task_a.task_id))
        DB.session.commit()

    # wait till it dies
    actual = scheduler.executor.get_actual_submitted_or_running(
        list(scheduler._submitted_or_running.keys())
    )
    while actual:
        time.sleep(1)
        actual = scheduler.executor.get_actual_submitted_or_running(
            list(scheduler._submitted_or_running.keys())
        )
    scheduler.executor.stop(list(scheduler._submitted_or_running.keys()))

    # make sure no more heartbeats were registered
    with app.app_context():
        ti = DB.session.query(TaskInstance).filter_by(task_id=task_a.task_id
                                                      ).one()
        assert ti.report_by_date < max_heartbeat


def test_ti_error_state(db_cfg, client_env):
    """test that a task that fails moves into error state"""
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # setup workflow
    tool = Tool()
    workflow = tool.create_workflow(name="test_ti_error_state")
    executor = SequentialExecutor()
    workflow.set_executor(executor)
    task_a = BashTask("exit -9", executor_class="SequentialExecutor",
                      max_attempts=1)
    workflow.add_task(task_a)

    # run it
    wfr = workflow.run()

    # check that the task is in failed state
    assert wfr.swarm_tasks[task_a.task_id].status == TaskStatus.ERROR_FATAL

    # check that the task instance is in error state
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        ti = DB.session.query(TaskInstance).filter_by(task_id=task_a.task_id
                                                      ).one()
        assert ti.status == TaskInstanceStatus.ERROR


def mock_execute(command, name, executor_parameters):
    # redirecting the normal route of going to B state with W state
    return -33333


def test_ti_w_state(db_cfg, client_env):
    """test that a task moves into 'W' state if it gets -333333 from the
    executor"""
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler

    # setup workflow
    tool = Tool()
    workflow = tool.create_workflow(name="test_ti_error_state")
    executor = SequentialExecutor()
    workflow.set_executor(executor)
    task_a = BashTask("exit -9", executor_class="SequentialExecutor",
                      max_attempts=1)
    workflow.add_task(task_a)

    # bind workflow to db
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # queue task
    swarm_task = wfr.swarm_tasks[task_a.task_id]
    wfr._adjust_resources_and_queue(swarm_task)

    requester = Requester(client_env)
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)

    # patch register submission to go into 'W' state
    with patch.object(executor, "execute", mock_execute):

        # try and schedule the job
        scheduler.executor.start()
        scheduler.schedule()

    # make sure no more heartbeats were registered
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        ti = DB.session.query(TaskInstance).filter_by(task_id=task_a.task_id
                                                      ).one()
        assert ti.status == TaskInstanceStatus.NO_EXECUTOR_ID


@pytest.mark.qsubs_jobs
def test_reset_attempts_on_resume(db_cfg, client_env):
    """test that num attempts gets reset on a resume"""

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # setup workflow 1
    tool = Tool()
    workflow1 = tool.create_workflow(name="test_reset_attempts_on_resume")
    executor = SequentialExecutor()
    workflow1.set_executor(executor)
    task_a = BashTask("sleep 5", executor_class="SequentialExecutor",
                      max_attempts=3)
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    wfr_1 = workflow1._create_workflow_run()
    wfr_1.update_status(WorkflowRunStatus.ERROR)

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE task
            SET status='{s}', num_attempts=3, max_attempts=3
            WHERE task.id={task_id}""".format(s=TaskStatus.ERROR_FATAL,
                                              task_id=task_a.task_id))
        DB.session.commit()

    # create a second workflow and actually run it
    workflow2 = tool.create_workflow(name="test_reset_attempts_on_resume",
                                     workflow_args=workflow1.workflow_args)
    executor = SequentialExecutor()
    workflow2.set_executor(executor)
    task_a = BashTask("sleep 5", executor_class="SequentialExecutor",
                      max_attempts=3)
    workflow2.add_task(task_a)
    wfr_2 = workflow2.run(resume=True)

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        t = DB.session.query(Task).filter_by(id=task_a.task_id).one()
        assert t.max_attempts == 3
        assert t.num_attempts == 1
        assert t.status == TaskStatus.DONE
        DB.session.commit()

    # Validate that a new WorkflowRun was created and is DONE
    assert wfr_1.workflow_run_id != wfr_2.workflow_run_id
    with app.app_context():
        wf = DB.session.query(Workflow).filter_by(id=workflow2.workflow_id
                                                  ).one()
        assert wf.status == WorkflowStatus.DONE

        wfrs = DB.session.query(WorkflowRun).filter_by(
            workflow_id=workflow2.workflow_id).all()
        assert len(wfrs) == 2
        DB.session.commit()


def test_task_instance_error_fatal(db_cfg, client_env):
    """test that num attempts gets reset on a resume"""

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # setup workflow 1
    tool = Tool()
    workflow1 = tool.create_workflow(name="test_task_instance_error_fatal")
    executor = SequentialExecutor()
    workflow1.set_executor(executor)
    task_a = BashTask("sleep 5", executor_class="SequentialExecutor",
                      max_attempts=3)
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    wfr_1 = workflow1._create_workflow_run()

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # fake workflow run
        DB.session.execute("""
            UPDATE workflow_run
            SET status ='{s}'
            WHERE id={wfr_id}""".format(s=WorkflowRunStatus.RUNNING,
                                        wfr_id=wfr_1.workflow_run_id))
        DB.session.execute("""
            INSERT INTO task_instance (workflow_run_id, task_id, status)
            VALUES ({wfr_id}, {t_id}, '{s}')""".format(
                wfr_id=wfr_1.workflow_run_id,
                t_id=task_a.task_id,
                s=TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR))
        ti = DB.session.execute(
            "SELECT max(id) from task_instance where task_id={}".format(task_a.task_id)
        ).fetchone()
        ti_id = ti[0]
        DB.session.execute("""
            UPDATE task
            SET status ='{s}'
            WHERE id={t_id}""".format(s=TaskStatus.RUNNING,
                                      t_id=task_a.task_id))
        DB.session.commit()

    # log task_instance fatal error
    app_route = f"/worker/task_instance/{ti_id}/log_error_worker_node"
    return_code, _ = workflow1.requester.send_request(
        app_route=app_route,
        message={"error_state": "F", "error_message": "bla bla bla"},
        request_type='post'
    )
    assert return_code == 200
    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        t = DB.session.query(Task).filter_by(id=task_a.task_id).one()
        assert t.status == TaskStatus.ERROR_FATAL
        DB.session.commit()
