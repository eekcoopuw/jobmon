import os
import pytest
from unittest.mock import patch

from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor
from jobmon.client.execution.strategies.sge.sge_executor import ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES as ecir
from jobmon.exceptions import RemoteExitInfoNotAvailable, ReturnCodes
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.client.templates.bash_task import BashTask

path_to_file = os.path.dirname(__file__)


def mock_do_nothing():
    """This is an empty method used for mock"""
    pass

def mock_qstat():
    """mock jobmon.client.execution.strategies.sge.sge_utils.qstat to return a fixed job list with one fixed job"""
    job = {'job_id': 66666,
           'hostname': 'mewo.cat.org',
           'name': 'test1',
           'user': 'mimi',
           'slots': 1,
           'status': 'D',
           'status_start': '03/01/2020 11:11:11',
           'runtime': 10000,
           'runtime_seconds': 10}
    return {'66666': job}


def mock_qacct_exit_status(id):
    """mock jobmon.client.execution.strategies.sge.sge_utils.qacct_exit_status to
    return some fixed code and msg according to id"""
    if id == 100:
        return ecir[0], "over runtime"
    if id == 200:
        return ecir[1], "I run out of ideas of fake msgs"
    if id == 300:
        return ReturnCodes.WORKER_NODE_ENV_FAILURE, "I run out of ideas of fake msgs again"
    if id == 400:
        return 66666, "This should throw an exception."


@pytest.mark.unittest
def test_get_actual_submitted_or_running():
    """This is to test the get_actual_submitted_or_running returns a list of int"""
    with patch("jobmon.client.execution.strategies.sge.sge_utils.qstat") as m_qstat:
        m_qstat.side_effect = mock_qstat
        sge = SGEExecutor()
        result = sge.get_actual_submitted_or_running()
        assert type(result) is list
        assert len(result) == 1
        assert result[0] == 66666


@pytest.mark.unittest
@pytest.mark.parametrize("id, ex", [(100, (TaskInstanceStatus.RESOURCE_ERROR, "max_runtime")),
                                    (200, (TaskInstanceStatus.RESOURCE_ERROR, "Insufficient resources")),
                                    (300, (TaskInstanceStatus.UNKNOWN_ERROR, "discrepancy")),
                                    (400, ('e', RemoteExitInfoNotAvailable))])
def test_get_remote_exit_info(id, ex):
    """This test goes through the if else branches of get_remote_exit_info"""
    with patch("jobmon.client.execution.strategies.sge.sge_utils.qacct_exit_status") as m_qacct_exit_status:
        m_qacct_exit_status.side_effect = mock_qacct_exit_status
        sge = SGEExecutor()
        if ex[0] == 'e':
            with pytest.raises(RemoteExitInfoNotAvailable):
                assert sge.get_remote_exit_info(id)
        else:
            result, msg = sge.get_remote_exit_info(id)
            assert result == ex[0]
            assert ex[1] in msg


@pytest.mark.unittest
def test_build_qsub_command():
    # basic
    r = SGEExecutor()._build_qsub_command( base_cmd="date",
                                           name="test",
                                           mem=0.5,
                                           cores=4,
                                           queue="all.q",
                                           runtime=10000,
                                           j=False,
                                           context_args={})
    assert "qsub  -N test -q 'all.q' -l fthread=4  -l m_mem_free=0.5G -l h_rt=10000 -P ihme_general    -V" in r
    assert "\"date\"" in r
    # dir
    with patch("cluster_utils.io.makedirs_safely") as m_makedirs_safely:
        m_makedirs_safely.side_effect = mock_do_nothing
        r = SGEExecutor()._build_qsub_command(base_cmd="date",
                                              name="test",
                                              mem=4,
                                              cores=4,
                                              queue="long.q",
                                              runtime=10000,
                                              j=False,
                                              context_args={},
                                              stderr="/ihme",
                                              stdout="/ihme/homes",
                                              project="proj_test",
                                              working_dir="/working_dir")
        assert "qsub -wd /working_dir -N test -q \'long.q\' -l fthread=4  -l m_mem_free=4G -l h_rt=10000 -P " \
               "proj_test -e /ihme -o /ihme/homes  -V" in r
        assert "\"date\"" in r
        # context_args
        with patch("cluster_utils.io.makedirs_safely") as m_makedirs_safely:
            m_makedirs_safely.side_effect = mock_do_nothing
            r = SGEExecutor()._build_qsub_command(base_cmd="date",
                                                  name="test",
                                                  mem=40,
                                                  cores=1,
                                                  queue="i.q",
                                                  runtime=10,
                                                  j=True,
                                                  context_args={"sge_add_args": "whatever"},
                                                  stderr="/ihme",
                                                  stdout="/ihme/homes",
                                                  project="proj_test",
                                                  working_dir="/working_dir")
            assert "qsub -wd /working_dir -N test -q \'i.q\' -l fthread=1 -l archive=TRUE -l m_mem_free=40G -l h_rt=10 -P proj_test " \
                   "-e /ihme -o /ihme/homes whatever -V" in r
            assert "\"date\"" in r


class MockSchedulerProc:
    def is_alive(self):
        return True

@pytest.mark.systemtest
def test_instantiation(db_cfg, client_env):
    from jobmon.client.execution.strategies._sgesimulator._test_unknown_workflow import _TestUnknownWorkflow as Workflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="_SimulatorSGEExecutor")
    workflow = Workflow("my_simple_dag",
                               executor_class="_SimulatorSGEExecutor",
                               seconds_until_timeout=30)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id,
                                      wfr.workflow_run_id,
                                      workflow._executor)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

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


@pytest.mark.smoketest
@pytest.mark.systemtest
def test_workflow(db_cfg, client_env):
    from jobmon.client.execution.strategies._sgesimulator._test_unknown_workflow import _TestUnknownWorkflow as Workflow
    task = BashTask(command=f"{os.path.join(path_to_file, 'jmtest.sh')}",
                    executor_class="_SimulatorSGEExecutor",
                    name="test",
                    num_cores=2,
                    max_runtime_seconds=10,
                    m_mem_free='1G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    resource = task.executor_parameters()
    resource.validate()
    workflow = Workflow("test", project='proj_scicomp', executor_class="_SimulatorSGEExecutor", seconds_until_timeout=30)
    workflow.add_tasks([task])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            SELECT status, executor_type
            FROM task_instance
            WHERE task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "D"
    assert res[1] == "_SimulatorSGEExecutor"


@pytest.mark.smoketest
@pytest.mark.systemtest
def test_workflow_timeout(db_cfg, client_env):
    from jobmon.client.execution.strategies._sgesimulator._test_unknown_workflow import _TestUnknownWorkflow as Workflow
    task = BashTask(command="sleep 60",
                    executor_class="_SimulatorSGEExecutor",
                    name="test",
                    num_cores=1,
                    max_runtime_seconds=10,
                    m_mem_free='1G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    resource = task.executor_parameters()
    resource.validate()
    workflow = Workflow("test", project='proj_scicomp', executor_class="_SimulatorSGEExecutor", seconds_until_timeout=10)
    workflow.add_tasks([task])
    with pytest.raises(RuntimeError):
        workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            select task_instance.status, task.status, workflow_run.status, workflow.status 
            from task_instance, task, workflow_run, workflow 
            where task_instance.task_id=task.id 
            and task_instance.workflow_run_id=workflow_run.id 
            and workflow_run.workflow_id=workflow.id 
            and task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "R"
    assert res[1] == "R"
    assert res[2] == "E"
    assert res[3] == "F"


@pytest.mark.smoketest
@pytest.mark.systemtest
def test_workflow_137(db_cfg, client_env):
    from jobmon.client.execution.strategies._sgesimulator._test_unknown_workflow import _TestUnknownWorkflow as Workflow
    task = BashTask(command="echo 137",
                    executor_class="_SimulatorSGEExecutor",
                    name="test",
                    num_cores=1,
                    max_runtime_seconds=10,
                    m_mem_free='1G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    resource = task.executor_parameters()
    resource.validate()
    workflow = Workflow("test", project='proj_scicomp', executor_class="_SimulatorSGEExecutor", seconds_until_timeout=300)
    workflow.add_tasks([task])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            select task_instance.status, task.status, workflow_run.status, workflow.status 
            from task_instance, task, workflow_run, workflow 
            where task_instance.task_id=task.id 
            and task_instance.workflow_run_id=workflow_run.id 
            and workflow_run.workflow_id=workflow.id 
            and task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
        assert res[0] == "E"
        assert res[1] == "F"
        assert res[2] == "E"
        assert res[3] == "F"

        sql = "select count(*) from task_instance_error_log where task_instance_id={}".format(task.task_id)
        res = DB.session.execute(sql).fetchone()
        assert res[0] == 1
