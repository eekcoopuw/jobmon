import os
import pytest
import signal
import subprocess
from time import sleep
from multiprocessing import Process

from jobmon.client import BashTask
from jobmon.client import PythonTask
from jobmon.client import Workflow
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_run import WorkflowRun
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus, WorkflowStillRunning

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


def resumable_workflow():
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow
    t1 = BashTask("sleep infinity", num_cores=1)
    wfa = "my_simple_dag"
    workflow = Workflow(wfa, project="proj_tools", resume=True)
    workflow.add_tasks([t1])
    return workflow


def run_workflow():
    workflow = resumable_workflow()
    workflow.execute()


def test_resume_running_workflow_fails(db_cfg, env_var):
    teardown_db(db_cfg)
    # create a workflow in a separate process with 1 job that sleeps forever.
    # it must be in a separate process because resume will kill the process
    # that the workflow is running on which would terminate the test process
    p1 = Process(target=run_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGKILL)
        p1.terminate()
        teardown_db(db_cfg)
        return

    with pytest.raises(WorkflowStillRunning):
        run_workflow()

    # kill the workflow process and make sure that things get set to stopped
    os.kill(p1.pid, signal.SIGTERM)
    sleep (5)
    os.kill(p1.pid, signal.SIGKILL)

    wf_run_state = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                           f"grep -v grep", shell=True,
                                           universal_newlines=True)
    assert "Z" in wf_run_state # make sure workflow run got properly killed
    teardown_db(db_cfg)


def test_cold_resume_workflow(db_cfg, env_var):
    teardown_db(db_cfg)
    p1 = Process(target=run_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGKILL)
        os.kill(p1.pid, signal.SIGTERM)
        sleep(5)
        os.kill(p1.pid, signal.SIGKILL)
        teardown_db(db_cfg)
        return

    # need to manually set WFR and WF to stopped because health monitor is not
    # running
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wfr_update = f"""UPDATE workflow_run SET status = '{WorkflowRunStatus.STOPPED}'"""
        wf_update = f"""UPDATE workflow SET status = '{WorkflowStatus.STOPPED}'"""
        DB.session.execute(wfr_update)
        DB.session.execute(wf_update)
        DB.session.commit()
        resp = DB.session.execute("""SELECT status FROM workflow_run""")\
            .fetchall()
        assert resp[0][0] == WorkflowRunStatus.STOPPED

        resp2 = DB.session.execute("""SELECT status FROM workflow""")\
            .fetchall()
        assert resp2[0][0] == WorkflowStatus.STOPPED

    p2 = Process(target=run_workflow)
    p2.start()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        num_wfr = 0
        count = 0
        while num_wfr != 2 and count < 10:
            wfruns = DB.session.query(WorkflowRun).all()
            num_wfr = len(wfruns)
            DB.session.commit()
            count = count + 1
            sleep(5)
        assert len(wfruns) == 2
    wfr_status_query = f"""SELECT status FROM workflow_run"""
    job_instance_query = f"""SELECT status, executor_id FROM job_instance"""

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_run_status = DB.session.execute(wfr_status_query).fetchall()
        assert len(wf_run_status) == 2
        assert wf_run_status[0][0] == WorkflowRunStatus.COLD_RESUME
        assert wf_run_status[1][0] == WorkflowRunStatus.RUNNING

        wf_status = DB.session.execute("""SELECT status FROM workflow""")\
            .fetchall()
        assert len(wf_status) == 1
        assert (wf_status)[0][0] == WorkflowStatus.RUNNING

        job_instances = DB.session.execute(job_instance_query).fetchall()
        count = 5
        while len(job_instances) != 2 and count > 0:
            sleep(5)
            count = count - 1
            DB.session.commit()
            job_instances = DB.session.execute(job_instance_query).fetchall()
        assert len(job_instances) == 2
        assert job_instances[0].status == JobInstanceStatus.KILL_SELF
        ji_state = subprocess.check_output(f'qacct -j '
                                           f'{job_instances[0].executor_id}',
                                           shell=True, universal_newlines=True)
        # make sure that the old job instance is dead
        assert '137' in ji_state.split('exit_status')[1].split('\n')[0]

        job_status = DB.session.execute("""SELECT status FROM job""")\
            .fetchall()
        assert len(job_status) == 1
        if job_status[0][0] == JobStatus.RUNNING:
            subprocess.check_output(f"qdel {job_instances[1].executor_id}",
                                    shell=True, universal_newlines=True)
        else:
            assert job_status[0][0] == JobStatus.INSTANTIATED

    wf_run1_teardown = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                               f"grep -v grep", shell=True,
                                               universal_newlines=True)
    assert 'Z+' in wf_run1_teardown # make sure the initial workflow run exited

    os.kill(p1.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p1.pid, signal.SIGKILL)
    os.kill(p2.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p2.pid, signal.SIGKILL)

    teardown_db(db_cfg)


def hot_resumable_workflow():
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow
    t1 = BashTask("sleep infinity", num_cores=1)
    wfa = "my_simple_dag"
    workflow = Workflow(wfa, project="proj_tools", resume=True,
                        reset_running_jobs=False)
    workflow.add_tasks([t1])
    return workflow


def run_hot_resumable_workflow():
    workflow = hot_resumable_workflow()
    workflow.execute()


def test_hot_resume_workflow(db_cfg, env_var):
    teardown_db(db_cfg)
    p1 = Process(target=run_hot_resumable_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGTERM)
        sleep(5)
        os.kill(p1.pid, signal.SIGKILL)
        p1.terminate()
        teardown_db(db_cfg)
        return

    # need to manually set WFR and WF to stopped because health monitor is not
    # running
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wfr_update = f"""UPDATE workflow_run SET status = '{WorkflowRunStatus.STOPPED}'"""
        wf_update = f"""UPDATE workflow SET status = '{WorkflowStatus.STOPPED}'"""
        DB.session.execute(wfr_update)
        DB.session.execute(wf_update)
        DB.session.commit()
        resp = DB.session.execute("""SELECT status FROM workflow_run""") \
            .fetchall()
        assert resp[0][0] == WorkflowRunStatus.STOPPED

        resp2 = DB.session.execute("""SELECT status FROM workflow""") \
            .fetchall()
        assert resp2[0][0] == WorkflowStatus.STOPPED

    p2 = Process(target=run_hot_resumable_workflow)
    p2.start()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        num_wfr = 0
        count = 0
        while num_wfr != 2 and count < 10:
            wfruns = DB.session.query(WorkflowRun).all()
            num_wfr = len(wfruns)
            DB.session.commit()
            count = count + 1
            sleep(5)
        assert len(wfruns) == 2
    wfr_status_query = f"""SELECT status FROM workflow_run"""
    job_instance_query = f"""SELECT status, executor_id FROM job_instance"""

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_run_status = DB.session.execute(wfr_status_query).fetchall()
        assert len(wf_run_status) == 2
        assert wf_run_status[0][0] == WorkflowRunStatus.HOT_RESUME
        assert wf_run_status[1][0] == WorkflowRunStatus.RUNNING

        wf_status = DB.session.execute("""SELECT status FROM workflow""") \
            .fetchall()
        assert len(wf_status) == 1
        assert (wf_status)[0][0] == WorkflowStatus.RUNNING

        # make sure running job instance did not get stopped and job continued
        # to run
        job_instances = DB.session.execute(job_instance_query).fetchall()
        assert len(job_instances) == 1
        assert job_instances[0].status == JobInstanceStatus.RUNNING

        job_status = DB.session.execute(
            """SELECT status FROM job""").fetchall()
        assert len(job_status) == 1
        assert job_status[0][0] == JobStatus.RUNNING

        # qdel job instance to make sure it gets cleaned up
        subprocess.check_output(f"qdel {job_instances[0].executor_id}",
                                shell=True, universal_newlines=True)

    wf_run1_teardown = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                               f"grep -v grep", shell=True,
                                               universal_newlines=True)
    assert 'Z+' in wf_run1_teardown # make sure the initial workflow run exited

    os.kill(p1.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p1.pid, signal.SIGKILL)
    os.kill(p2.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p2.pid, signal.SIGKILL)

    teardown_db(db_cfg)


def test_resource_scaling(db_cfg, env_var):
    from jobmon.client.swarm.executors import ExecutorParameters
    teardown_db(db_cfg)
    my_wf = Workflow(
        workflow_args="resource starved workflow",
        project="proj_tools")

    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=50,  # set max runtime to be shorter than task
        queue="all.q",
        executor_class="SGEExecutor")
    sleepy_task = BashTask(
        # set sleep to be longer than max runtime, forcing a retry
        "sleep 90",
        # job should succeed on second try. runtime will 150s on try 2
        max_attempts=3,
        executor_parameters=sleepy_params)
    my_wf.add_task(sleepy_task)

    # job will time out and get killed by the cluster. After a few minutes
    # jobmon will notice that it has disappeared and ask SGE for exit status.
    # SGE will show a resource kill. Jobmon will scale all resources by 30% and
    # retry the job at which point it will succeed.
    my_wf.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.query(Job).all()
        DB.session.commit()
        job = resp[0]
        assert len(job.job_instances) == 3
        assert job.status == "D"
    teardown_db(db_cfg)


def test_workflow_resume_new_resources(db_cfg, env_var):
    teardown_db(db_cfg)
    sge_params = ExecutorParameters(max_runtime_seconds=8,
                                    resource_scales={'m_mem_free': 0.2,
                                                     'max_runtime_seconds': 0.3})
    task = BashTask(name="rerun_task", command="sleep 10", max_attempts=1,
                    executor_parameters=sge_params)
    wf = Workflow(workflow_args="rerun_w_diff_resources", project="proj_tools")
    wf.add_task(task)

    wf.run()
    assert wf.status == 'E'

    sge_params2 = ExecutorParameters(max_runtime_seconds=40,
                                     resource_scales={'m_mem_free': 0.4,
                                                      'max_runtime_seconds': 0.5})

    task2 = BashTask(name="rerun_task", tag="new_tag", command="sleep 10",
                     max_attempts=2, executor_parameters=sge_params2)
    wf2 = Workflow(workflow_args="rerun_w_diff_resources",
                   project="ihme_general", resume=True)
    wf2.add_task(task2)
    wf2.run()
    assert wf2.status == 'D'

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name="rerun_task").first()
        assert job.executor_parameter_set.max_runtime_seconds == 40
        assert job.max_attempts == 2
        assert job.tag == 'new_tag'
        DB.session.commit()
    teardown_db(db_cfg)


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
