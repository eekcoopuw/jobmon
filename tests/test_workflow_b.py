import os
import pytest

from jobmon.client import BashTask
from jobmon.client import PythonTask
from jobmon.client import Workflow
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus

import tests.workflow_utils as wu
from tests.conftest import teardown_db

path_to_file = os.path.dirname(__file__)


def test_timeout(db_cfg, env_var):
    teardown_db(db_cfg)
    t1 = BashTask("sleep 10", num_cores=1)
    t2 = BashTask("sleep 11", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 12", upstream_tasks=[t2], num_cores=1)

    wfa1 = "timeout_dag"
    wf1 = Workflow(wfa1, seconds_until_timeout=3)
    wf1.add_tasks([t1, t2, t3])

    with pytest.raises(RuntimeError) as error:
        wf1.execute()

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (3 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)
    teardown_db(db_cfg)


def test_health_monitor_failing_nodes(db_cfg, env_var):
    """Test the Health Montior's identification of failing nodes"""
    teardown_db(db_cfg)
    # these dummy dags will increment the ID of our dag-of-interest to
    # avoid the timing collisions
    from jobmon.server.health_monitor.health_monitor import \
        HealthMonitor

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        for _ in range(5):
            DB.session.add(TaskDagMeta())
        DB.session.commit()

    t1 = BashTask("echo 'hello'", num_cores=1)
    t2 = BashTask("echo 'to'", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("echo 'the'", upstream_tasks=[t2], num_cores=1)
    t4 = BashTask("echo 'beautiful'", upstream_tasks=[t3], num_cores=1)
    t5 = BashTask("echo 'world'", upstream_tasks=[t4], num_cores=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], num_cores=1)
    workflow = Workflow("test_failing_nodes")
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.run()

    wfr = workflow.workflow_run

    hm = HealthMonitor(node_notification_sink=wu.mock_slack, app=app)
    hm._database = 'docker'

    # A database commit must follow each dbs update, sqlalchmey might use
    # a different dbs connection for each dbs statement. So the next query
    # might not see the because it will be on a different connection.
    # Therefore ensure that the update has hit the database by using a commit.
    with app.app_context():

        # Manually make the workflow run look like it's still running
        DB.session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.RUNNING,
                                             id=workflow.id))
        DB.session.commit()
        # This test's workflow should be in the 'active' AND succeeding list
        active_wfrs = hm._get_succeeding_active_workflow_runs(DB.session)
        assert wfr.id in active_wfrs

        # Manually make 5 job instances land on the same node & have them fail
        DB.session.execute("""
            UPDATE job_instance
            SET nodename='fake_node.ihme.washington.edu', status="{s}"
            WHERE job_instance_id < 7 and workflow_run_id={wfr_id}
            """.format(s=JobInstanceStatus.ERROR, wfr_id=wfr.id))
        DB.session.commit()
        failing_nodes = hm._calculate_node_failure_rate(DB.session,
                                                        active_wfrs)
        assert 'fake_node.ihme.washington.edu' in failing_nodes

        # Manually make those job instances land on the same node and have
        # them fail BUT also manually make their dates be older than an hour.
        # Ensure they they don't come up because of the time window
        DB.session.execute("""
            UPDATE job_instance
            SET nodename='new_fake_node.ihme.washington.edu', status="{s}",
            status_date = '2018-05-16 17:17:54'
            WHERE job_instance_id < 7 and workflow_run_id={wfr_id}
            """.format(s=JobInstanceStatus.ERROR, wfr_id=wfr.id))
        DB.session.commit()
        failing_nodes = hm._calculate_node_failure_rate(DB.session,
                                                        active_wfrs)
        assert 'new_fake_node.ihme.washington.edu' not in failing_nodes
    teardown_db(db_cfg)


def test_add_tasks_to_workflow(db_cfg, env_var):
    """Make sure adding tasks to a workflow (and not just a task dag) works"""
    teardown_db(db_cfg)
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "add_tasks_to_workflow"
    workflow = Workflow(workflow_args=wfa)
    workflow.add_tasks([t1, t2, t3])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        w = DB.session.query(WorkflowDAO).filter_by(id=workflow.id).first()
        assert w.status == 'D'
        j = DB.session.query(Job).\
            filter_by(dag_id=workflow.task_dag.dag_id).\
            all()
        assert all(t.status == 'D' for t in j)
        DB.session.commit()
    teardown_db(db_cfg)


def test_anonymous_workflow(db_cfg, env_var):
    # Make sure uuid is created for an anonymous workflow
    teardown_db(db_cfg)
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    workflow = Workflow()
    workflow.add_tasks([t1, t2, t3])
    workflow.run()
    bt3 = workflow.task_dag.job_list_manager.bound_task_from_task(t3)

    assert workflow.workflow_args is not None

    # Manually flip one of the jobs to Failed
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE workflow_run
            SET status='E'
            WHERE workflow_id={id}""".format(id=workflow.id))
        DB.session.execute("""
            UPDATE workflow
            SET status='E'
            WHERE id={id}""".format(id=workflow.id))
        DB.session.execute("""
            UPDATE job
            SET status='F'
            WHERE job_id={id}""".format(id=bt3.job_id))
        DB.session.commit()

    # Restart it using the uuid.
    uu_id = workflow.workflow_args
    new_workflow = Workflow(workflow_args=uu_id, resume=True)
    new_workflow.add_tasks([t1, t2, t3])
    new_workflow.run()

    # Make sure it's the same workflow
    assert workflow.id == new_workflow.id
    teardown_db(db_cfg)


def test_workflow_status_dates(db_cfg, simple_workflow):
    """Make sure the workflow status dates actually get updated"""
    wfid = simple_workflow.wf_dao.id
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_dao = DB.session.query(WorkflowDAO).filter_by(id=wfid).first()
        assert wf_dao.status_date != wf_dao.created_date

        wf_runs = wf_dao.workflow_runs
        for wfr in wf_runs:
            assert wfr.created_date != wfr.status_date
        DB.session.commit()
    teardown_db(db_cfg)


@pytest.mark.qsubs_jobs
def test_workflow_sge_args(db_cfg, env_var):
    """Test to make sure that the correct information is available to the
     worker cli. For some reason, there are times when this fails and whatever
     executor this workflow is pointing at has a working directory configured,
     but the executor that is actually qsubbing does not have a working
     directory configured. Is this because of the jqs and jsm interfering or a
     problem with the job instance reconcilers not accessing the correct
     executor somehow? """
    teardown_db(db_cfg)
    t1 = PythonTask(name="check_env",
                    script='{}/executor_args_check.py'
                    .format(os.path.dirname(os.path.realpath(__file__))),
                    num_cores=1, max_attempts=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "sge_args_dag"
    workflow = Workflow(workflow_args=wfa, project='proj_tools',
                        working_dir='/ihme/centralcomp/auto_test_data',
                        stderr='/tmp', stdout='/tmp')
    workflow.add_tasks([t1, t2, t3])
    wf_status = workflow.execute()

    # TODO Grab a ref to the executor

    # If the working directory is not set correctly then executor_args_check.py
    # will fail and write its error message into the job_instance_error_log
    # table.
    # This test is flakey. Gather more information by printing out the error.
    # TODO this was added in May 2019, if the test has ceased to be flakey
    # by July then remove this extra logging.
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    if wf_status != DagExecutionStatus.SUCCEEDED:
        print(">>>>>> FLAKEY TEST test_workflow_sge_args FAILED <<<<<<<<<<<<<")
        with app.app_context():
            query = (
                "SELECT job_instance_id, description  "
                "FROM job_instance_error_log ")
            results = DB.session.execute(query)
            for row in results:
                print("Error: jid {}, description {}".format(row[0], row[1]))
            DB.session.commit()

    assert workflow.workflow_run.project == 'proj_tools'
    assert workflow.workflow_run.working_dir == (
        '/ihme/centralcomp/auto_test_data')
    assert workflow.workflow_run.stderr == '/tmp'
    assert workflow.workflow_run.stdout == '/tmp'
    assert workflow.workflow_run.executor_class == 'SGEExecutor'
    assert wf_status == DagExecutionStatus.SUCCEEDED
    teardown_db(db_cfg)


def test_workflow_identical_args(db_cfg, env_var):
    teardown_db(db_cfg)
    # first workflow runs and finishes
    wf1 = Workflow(workflow_args="same", project='proj_tools')
    task = BashTask("sleep 2", num_cores=1)
    wf1.add_task(task)
    wf1.execute()

    # tries to create an identical workflow without the restart flag
    wf2 = Workflow(workflow_args="same", project='proj_tools')
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.execute()

    # creates a workflow, okayed to restart, but original workflow is done
    wf3 = Workflow(workflow_args="same", project='proj_tools',
                   resume=ResumeStatus.RESUME)
    wf3.add_task(task)
    with pytest.raises(WorkflowAlreadyComplete):
        wf3.execute()
    teardown_db(db_cfg)


def test_same_wf_args_diff_dag(db_cfg, env_var):
    teardown_db(db_cfg)
    wf1 = Workflow(workflow_args="same", project='proj_tools')
    task1 = BashTask("sleep 2", num_cores=1)
    wf1.add_task(task1)

    wf2 = Workflow(workflow_args="same", project='proj_tools')
    task2 = BashTask("sleep 3", num_cores=1)
    wf2.add_task(task2)

    exit_status = wf1.execute()

    assert exit_status == 0

    with pytest.raises(WorkflowAlreadyExists):
        wf2.run()
    teardown_db(db_cfg)


def test_workflow_config_reconciliation():
    Workflow(name="test_reconciliation_args", reconciliation_interval=3,
                  heartbeat_interval=4, report_by_buffer=5.1)
    from jobmon.client import client_config
    assert client_config.report_by_buffer == 5.1
    assert client_config.heartbeat_interval == 4
    assert client_config.reconciliation_interval == 3


def test_workflow_in_running_state(db_cfg, env_var):
    teardown_db(db_cfg)
    t1 = BashTask("sleep 10", executor_class="SequentialExecutor",
                  max_runtime_seconds=15, resource_scales={})
    workflow = Workflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow.task_dag._execute()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wfDAO = DB.session.query(WorkflowDAO).filter_by(id=workflow.id).first()
        assert wfDAO.status == WorkflowStatus.RUNNING
        DB.session.commit()
    teardown_db(db_cfg)
