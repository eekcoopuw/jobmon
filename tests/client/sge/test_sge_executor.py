import os
import pytest
from unittest.mock import patch

from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor
from jobmon.constants import QsubAttribute, TaskInstanceStatus
from jobmon.exceptions import RemoteExitInfoNotAvailable, ReturnCodes

path_to_file = os.path.dirname(__file__)


def mock_do_nothing():
    """This is an empty method used for mock"""
    pass


def mock_qstat():
    """mock jobmon.client.execution.strategies.sge.sge_utils.qstat to return a fixed job list
    with one fixed job"""
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
        return QsubAttribute.ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES[0], "over runtime"
    if id == 200:
        return QsubAttribute.ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES[1], "I run out of ideas of fake msgs"
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
@pytest.mark.parametrize(
    "id, ex", [(100, (TaskInstanceStatus.RESOURCE_ERROR, "max_runtime")),
               (200, (TaskInstanceStatus.RESOURCE_ERROR, "Insufficient resources")),
               (300, (TaskInstanceStatus.ERROR_FATAL, "discrepancy")),
               (400, ('e', RemoteExitInfoNotAvailable))])
def test_get_remote_exit_info(id, ex):
    """This test goes through the if else branches of get_remote_exit_info"""
    with patch("jobmon.client.execution.strategies.sge.sge_utils.qacct_exit_status"
               ) as m_qacct_exit_status:
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
    r = SGEExecutor()._build_qsub_command(base_cmd="date",
                                          name="test",
                                          mem=0.5,
                                          cores=4,
                                          queue="all.q",
                                          runtime=10000,
                                          j=False,
                                          context_args={})
    assert ("qsub  -N test -q 'all.q' -l fthread=4  -l m_mem_free=0.5G -l h_rt=10000 -P "
            "ihme_general   -w e  -V") in r
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
                                              stderr="~",
                                              stdout="~",
                                              project="proj_test",
                                              working_dir="/working_dir")
        assert ("qsub -wd /working_dir -N test -q \'long.q\' -l fthread=4  -l m_mem_free=4G "
                "-l h_rt=10000 -P proj_test -e ~ -o ~ -w e  -V") in r
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
                                                  stderr="~",
                                                  stdout="~",
                                                  project="proj_test",
                                                  working_dir="/working_dir")
            assert ("qsub -wd /working_dir -N test -q \'i.q\' -l fthread=1 -l archive=TRUE -l "
                    "m_mem_free=40G -l h_rt=10 -P proj_test -e ~ -o ~ -w e whatever -V") in r
            assert "\"date\"" in r


class MockSchedulerProc:
    def is_alive(self):
        return True


@pytest.mark.systemtest
@pytest.mark.skip(reason="need executor plugin api to use _sgesimulator")
def test_instantiation(db_cfg, requester_no_retries):
    from tests.client.sge._sgesimulator._test_unknown_workflow import (_TestUnknownWorkflow as
                                                                       Workflow)
    from jobmon.client.api import BashTask
    from jobmon.client.execution.scheduler.task_instance_scheduler import TaskInstanceScheduler

    t1 = BashTask("echo 1", executor_class="_SimulatorSGEExecutor")
    workflow = Workflow(executor_class="_SimulatorSGEExecutor",
                        seconds_until_timeout=30)
    workflow.add_tasks([t1])
    workflow._bind()
    wfr = workflow._create_workflow_run()
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester_no_retries)
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
@pytest.mark.skip(reason="need executor plugin api to use _sgesimulator")
def test_workflow(db_cfg, client_env):
    from tests.client.sge._sgesimulator._test_unknown_workflow import (_TestUnknownWorkflow as
                                                                       Workflow)
    from jobmon.client.api import BashTask
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
    workflow = Workflow(project='proj_scicomp', executor_class="_SimulatorSGEExecutor",
                        seconds_until_timeout=30)
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
    from tests.client.sge._sgesimulator._test_unknown_workflow import (_TestUnknownWorkflow as
                                                                       Workflow)
    from jobmon.client.api import BashTask
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
    workflow = Workflow(project='proj_scicomp', executor_class="_SimulatorSGEExecutor",
                        seconds_until_timeout=10)
    workflow.add_tasks([task])
    with pytest.raises(RuntimeError):
        workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            select workflow.status
            from task_instance, task, workflow_run, workflow
            where task_instance.task_id=task.id
            and task_instance.workflow_run_id=workflow_run.id
            and workflow_run.workflow_id=workflow.id
            and task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "F"


@pytest.mark.systemtest
@pytest.mark.skip(reason="need executor plugin api to use _sgesimulator")
def test_workflow_137(db_cfg, client_env):
    from tests.client.sge._sgesimulator._test_unknown_workflow import \
        _TestUnknownWorkflow as Workflow
    from jobmon.client.api import BashTask
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
    workflow = Workflow(project='proj_scicomp',
                        executor_class="_SimulatorSGEExecutor",
                        seconds_until_timeout=300)
    workflow.add_tasks([task])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            SELECT
                task_instance.status, task.status, workflow_run.status,
                workflow.status
            FROM
                task_instance, task, workflow_run, workflow
            WHERE
                task_instance.task_id=task.id
                AND task_instance.workflow_run_id=workflow_run.id
                AND workflow_run.workflow_id=workflow.id
                AND task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
        assert res[0] == "E"
        assert res[1] == "F"
        assert res[2] == "E"
        assert res[3] == "F"

        sql = """
            SELECT
                count(*)
            FROM
                task_instance_error_log
            JOIN
                task_instance
                ON task_instance.id = task_instance_error_log.task_instance_id
            WHERE
                task_instance.task_id={}""".format(task.task_id)
        res = DB.session.execute(sql).fetchone()
        assert res[0] == 1


@pytest.mark.integration_sge
def test_sge_workflow_one_task(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    workflow = UnknownWorkflow(executor_class="SGEExecutor")
    task_a = BashTask(
        "echo a", executor_class="SGEExecutor",
        queue="long.q",
        upstream_tasks=[]  # To be clear
    )
    workflow.add_task(task_a)
    wfr = workflow.run()
    assert wfr.status == "D"
    assert wfr.completed_report[0] == 1
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 0


@pytest.mark.integration_sge
def test_sge_workflow_three_tasks(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    workflow = UnknownWorkflow("test_sge_three_linear_tasks",
                               executor_class="SGEExecutor")
    task_a = BashTask(
        "echo a", executor_class="SGEExecutor",
        queue="long.q",
        upstream_tasks=[]  # To be clear
    )
    workflow.add_task(task_a)
    task_b = BashTask(
        "echo b", executor_class="SGEExecutor",
        queue="long.q",
        upstream_tasks=[task_a]
    )
    workflow.add_task(task_b)
    task_c = BashTask("echo c", executor_class="SGEExecutor", queue="long.q")
    workflow.add_task(task_c)
    task_c.add_upstream(task_b)  # Exercise add_upstream post-instantiation
    wfr = workflow.run()
    assert wfr.status == "D"
    assert wfr.completed_report[0] == 3
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 0


@pytest.mark.integration_sge
def test_sge_workflow_timeout(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.api import BashTask
    task = BashTask(command="sleep 20",
                    executor_class="SGEExecutor",
                    name="test_timeout",
                    num_cores=1,
                    max_runtime_seconds=10,
                    m_mem_free='1G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    resource = task.executor_parameters()
    resource.validate()
    workflow = Workflow(project='proj_scicomp', executor_class="SGEExecutor",
                        seconds_until_timeout=10)
    workflow.add_tasks([task])
    with pytest.raises(RuntimeError):
        workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            select workflow.status
            from task_instance, task, workflow_run, workflow
            where task_instance.task_id=task.id
            and task_instance.workflow_run_id=workflow_run.id
            and workflow_run.workflow_id=workflow.id
            and task_id = :task_id"""
        res = DB.session.execute(sql, {"task_id": task.task_id}).fetchone()
        DB.session.commit()
    assert res[0] == "F"


@pytest.mark.integration_sge
def test_reconciler_sge_new_heartbeats(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.api import BashTask
    task = BashTask(command="sleep 10",
                    executor_class="SGEExecutor",
                    name="test_hearbeats",
                    num_cores=1,
                    max_runtime_seconds=70,
                    m_mem_free='1G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    resource = task.executor_parameters()
    resource.validate()
    workflow = Workflow(project='proj_scicomp', executor_class="SGEExecutor",
                        seconds_until_timeout=70)
    workflow.add_tasks([task])
    workflow.run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT submitted_date, report_by_date
        FROM task_instance
        WHERE task_id = {}""".format(task.task_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged


@pytest.mark.integration_sge
def test_no_suitable_queue(db_cfg, client_env):
    """This test submits a job with more memory than any cluster queue can handle. It then
    checks that it was moved to all.q with 750gb of memory."""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.api import BashTask
    from jobmon.client.execution.strategies.sge.sge_queue import SGE_ALL_Q

    SGE_ALL_Q.max_memory_gb = 1000000

    task = BashTask(command="sleep 10",
                    executor_class="SGEExecutor",
                    name="a_million_gigs",
                    num_cores=1,
                    max_runtime_seconds=70,
                    m_mem_free='1000000G',
                    max_attempts=1,
                    queue="all.q",
                    j_resource=True)
    workflow = Workflow(project='proj_scicomp', executor_class="SGEExecutor",
                        seconds_until_timeout=70)
    workflow.add_tasks([task])
    workflow.run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT status
        FROM task_instance
        WHERE task_id = {}""".format(task.task_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    assert res[0] == "W"

    SGE_ALL_Q.max_memory_gb = 1010


@pytest.mark.integration_sge
def test_eqw_restarting(db_cfg, client_env):
    """This test creates a task that will be moved in to eqw state by the cluster. It then
    checks that the task instance changes state."""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    # this directory exists but its perms wont let it be written to, this should cause eqw
    unwriteable_dir = "/ihme/scratch/users/svcscicompci/unwriteable_test_dir"

    workflow = UnknownWorkflow(project="proj_scicomp", executor_class="SGEExecutor",
                               seconds_until_timeout=3000,
                               stdout=unwriteable_dir,
                               stderr=unwriteable_dir)

    task1 = BashTask(command="sleep 10",
                     executor_class="SGEExecutor",
                     name="test_eqw_restarting",
                     num_cores=1,
                     max_runtime_seconds=3000,
                     m_mem_free="1G",
                     max_attempts=1,
                     queue="all.q")

    workflow.add_tasks([task1])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
            SELECT task_instance.status
            FROM task_instance
            WHERE task_instance.task_id = {}""".format(task1.task_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    assert res[0] == "F"

    with app.app_context():
        query = """
            SELECT task.status
            FROM task
            WHERE task.id = {}""".format(task1.task_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    assert res[0] == "F"
