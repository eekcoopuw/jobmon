import pytest
from time import sleep
import os
import uuid
import subprocess
from multiprocessing import Process

from jobmon import BashTask
from jobmon import PythonTask
from jobmon import StataTask
from jobmon import Workflow
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client import shared_requester as req
from jobmon.client import client_config
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus
from jobmon.client.utils import gently_kill_command


def cleanup_jlm(workflow):
    """If a dag is not completed properly and all threads are disconnected,
    a new job list manager will access old job instance factory/reconciler
    threads instead of creating new ones. So we need to make sure threads get
    cleand up at the end"""

    if workflow.task_dag.job_list_manager:
        workflow.task_dag.job_list_manager.disconnect()


@pytest.fixture
def fast_heartbeat():
    old_heartbeat = client_config.heartbeat_interval
    client_config.heartbeat_interval = 10
    yield
    client_config.heartbeat_interval = old_heartbeat


@pytest.fixture
def simple_workflow_w_errors(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 1", num_cores=1, mem_free='2G', queue="all.q",
                  j_resource=False)
    t2 = BashTask("not_a_command 1", upstream_tasks=[t1], num_cores=1,
                  mem_free='2G', queue="all.q", j_resource=False)
    t3 = BashTask("sleep 30", upstream_tasks=[t1], max_runtime_seconds=4,
                  num_cores=1, mem_free='2G', queue="all.q", j_resource=False)
    t4 = BashTask("not_a_command 3", upstream_tasks=[t2, t3], num_cores=1,
                  mem_free='2G', queue="all.q", j_resource=False)

    workflow = Workflow("my_failing_args", project='ihme_general')
    workflow.add_tasks([t1, t2, t3, t4])
    workflow.execute()
    return workflow


def mock_slack(msg, channel):
    print("{} to be posted to channel: {}".format(msg, channel))


def test_wf_with_stata_temp_dir(real_jsm_jqs, db_cfg):
    t1 = StataTask(script='di "hello"', num_cores=1)
    t2 = StataTask(script='di "world"', upstream_tasks=[t1], num_cores=1)

    wf = Workflow("stata_temp_dir_test")
    wf.add_tasks([t1, t2])

    success = wf.run()
    assert success


@pytest.mark.qsubs_jobs
def test_wfargs_update(real_jsm_jqs, db_cfg):
    # Create identical dags
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    t4 = BashTask("sleep 1", num_cores=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], num_cores=1)
    t6 = BashTask("sleep 3", upstream_tasks=[t5], num_cores=1)

    wfa1 = "v1"
    wf1 = Workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "v2"
    wf2 = Workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


@pytest.mark.qsubs_jobs
def test_resource_arguments(real_jsm_jqs, db_cfg):
    """
    Test the parsing/serialization max run time and cores.
    90,000 seconds is deliberately longer than one day, testing a specific
    bug"""
    t1 = BashTask("sleep 10",
                  queue='all.q',
                  max_runtime_seconds=90_000,
                  num_cores=2)
    wf = Workflow("test_resource_arguments-{}".format(uuid.uuid4()))
    wf.add_tasks([t1])
    return_code = wf.execute()
    assert return_code == DagExecutionStatus.SUCCEEDED


@pytest.mark.qsubs_jobs
def test_dag_update(real_jsm_jqs, db_cfg):
    # Create different dags
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    t4 = BashTask("sleep 3", num_cores=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], num_cores=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], num_cores=1)

    wfa1 = "dag_update"
    wf1 = Workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "dag_update"
    wf2 = Workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


@pytest.mark.qsubs_jobs
def test_wfagrs_dag_update(real_jsm_jqs, db_cfg):
    # Create different dags
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    t4 = BashTask("sleep 3", num_cores=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], num_cores=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], num_cores=1)

    wfa1 = "wfargs_dag_update"
    wf1 = Workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "wfargs_dag_update"
    wf2 = Workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


@pytest.mark.qsubs_jobs
def test_stop_resume(db_cfg, simple_workflow, tmpdir):
    # Manually modify the database so that some mid-dag jobs appear in
    # a running / non-complete / non-error state
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.bound_tasks.items()]

    to_run_jid = job_ids[-1]
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job
            SET status='{s}'
            WHERE job_id={jid}""".format(s=JobStatus.ADJUSTING_RESOURCES,
                                         jid=to_run_jid))
        DB.session.execute("""
            UPDATE workflow
            SET status='{s}'
            WHERE id={id}""".format(s=WorkflowStatus.STOPPED,
                                    id=stopped_wf.id))
        DB.session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.STOPPED,
                                             id=stopped_wf.id))
        DB.session.execute("""
            DELETE FROM job_instance
            WHERE job_id={jid}""".format(s=JobStatus.ADJUSTING_RESOURCES,
                                         jid=to_run_jid))
        DB.session.commit()

    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "my_simple_dag"
    elogdir = str(tmpdir.mkdir("wf_elogs"))
    ologdir = str(tmpdir.mkdir("wf_ologs"))

    workflow = Workflow(wfa, stderr=elogdir, stdout=ologdir,
                        project='proj_tools', resume=ResumeStatus.RESUME)
    workflow.add_tasks([t1, t2, t3])
    workflow.execute()

    # TODO: FIGURE OUT WHETHER IT'S SENSIBLE TO CLEAR THE done/error
    # QUEUES AFTER THE __init__ _sync() call in job_list_manager...

    # Check that finished tasks aren't added to the top fringe
    assert workflow.task_dag.top_fringe == [t3]

    # TODO: Check that the user is prompted that they indeed want to resume...

    # Validate that the new workflow has the same ID as the 'stopped' one
    assert workflow.id == stopped_wf.id

    # Validate that a new WorkflowRun was created
    assert workflow.workflow_run.id != stopped_wf.workflow_run.id

    # Validate that the old WorkflowRun was stopped
    with app.app_context():
        wf_run = (DB.session.query(WorkflowRunDAO).filter_by(
            id=stopped_wf.workflow_run.id).first())
        assert wf_run.status == WorkflowRunStatus.STOPPED
        wf_run_jobs = DB.session.query(JobInstance).filter_by(
            workflow_run_id=stopped_wf.workflow_run.id).all()
        assert all(job.status != JobInstanceStatus.RUNNING
                   for job in wf_run_jobs)
        DB.session.commit()

    # Validate that a new WorkflowRun has different logdirs and project
    assert workflow.workflow_run.stderr != stopped_wf.workflow_run.stderr
    assert workflow.workflow_run.stdout != stopped_wf.workflow_run.stdout
    assert workflow.workflow_run.project != stopped_wf.workflow_run.project

    # Validate that the database indicates the Dag and its Jobs are complete
    assert workflow.status == WorkflowStatus.DONE

    jlm = workflow.task_dag.job_list_manager
    for _, task in workflow.task_dag.tasks.items():
        assert jlm.status_from_task(task) == JobStatus.DONE


@pytest.mark.qsubs_jobs
def test_reset_attempts_on_resume(db_cfg, simple_workflow):
    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.bound_tasks.items()]

    mod_jid = job_ids[1]

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job
            SET status='{s}', num_attempts=3, max_attempts=3
            WHERE job_id={jid}""".format(s=JobStatus.ERROR_FATAL,
                                         jid=mod_jid))
        DB.session.execute("""
            UPDATE job_instance
            SET status='{s}'
            WHERE job_id={jid}""".format(s=JobInstanceStatus.ERROR,
                                         jid=mod_jid))
        DB.session.execute("""
            UPDATE workflow
            SET status='{s}'
            WHERE id={id}""".format(s=WorkflowStatus.ERROR,
                                    id=stopped_wf.id))
        DB.session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.ERROR,
                                             id=stopped_wf.id))
        DB.session.commit()

    # Re-instantiate the DAG + Workflow
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "my_simple_dag"
    workflow = Workflow(wfa, resume=ResumeStatus.RESUME)
    workflow.add_tasks([t1, t2, t3])

    # Before actually executing the DAG, validate that the database has
    # reset the attempt counters to 0 and the ERROR states to INSTANTIATED
    workflow._bind()

    workflow.task_dag.job_list_manager._sync()
    bt2 = workflow.task_dag.job_list_manager.bound_task_from_task(t2)
    assert bt2.job_id == mod_jid  # Should be bound to stopped-run ID values

    with app.app_context():
        jobDAO = DB.session.query(Job).filter_by(job_id=bt2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 0
        assert jobDAO.status == JobStatus.ADJUSTING_RESOURCES
        DB.session.commit()

    workflow.execute()

    # TODO: Check that the user is prompted that they want to resume...

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        jobDAO = DB.session.query(Job).filter_by(job_id=bt2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 1
        assert jobDAO.status == JobStatus.DONE
        DB.session.commit()

    # Validate that a new WorkflowRun was created and is DONE
    assert workflow.workflow_run.id != stopped_wf.workflow_run.id
    with app.app_context():
        wfDAO = DB.session.query(WorkflowDAO).filter_by(id=workflow.id).first()
        assert wfDAO.status == WorkflowStatus.DONE

        wfrDAOs = DB.session.query(WorkflowRunDAO).filter_by(
            workflow_id=workflow.id).all()
        assert len(wfrDAOs) == 2

        done_wfr = [wfrd for wfrd in wfrDAOs
                    if wfrd.id == workflow.workflow_run.id][0]
        other_wfr = [wfrd for wfrd in wfrDAOs
                     if wfrd.id != workflow.workflow_run.id][0]
        assert done_wfr.status == WorkflowRunStatus.DONE

        # TODO: Improve design for STOPPED/ERROR states for both Workflows and
        # WorkflowRuns..
        assert other_wfr.status == WorkflowRunStatus.STOPPED
        DB.session.commit()


@pytest.mark.qsubs_jobs
def test_attempt_resume_on_complete_workflow(simple_workflow):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "my_simple_dag"
    workflow = Workflow(wfa, resume=ResumeStatus.RESUME)
    workflow.add_tasks([t1, t2, t3])

    with pytest.raises(WorkflowAlreadyComplete):
        workflow.execute()


def test_force_new_workflow_instead_of_resume(simple_workflow):
    # TODO (design): Is there ever a scenario where this is a good thing to do?
    # This is more or less possible by updating WorkflowArgs... which I think
    # is better practice than trying to create a new Workflow with identical
    # args and DAG, which is a violation of our current concept of Workflow
    # uniqueness. If we really want to all this behavior, we need to further
    # refine that concept and potentially add another piece of information
    # to the Workflow hash itself.
    pass


def test_dag_reset(db_cfg, simple_workflow_w_errors):
    # Alias to shorter names...
    err_wf = simple_workflow_w_errors

    dag_id = err_wf.task_dag.dag_id

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        jobs = DB.session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.ERROR_FATAL,
                     JobStatus.ERROR_FATAL, JobStatus.ADJUSTING_RESOURCES]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))
        DB.session.commit()

    # Now RESET and make sure all the jobs that aren't "DONE" flip back to
    # ADJUSTING_RESOURCES
    rc, _ = req.send_request(
        app_route='/task_dag/{}/reset_incomplete_jobs'.format(dag_id),
        message={},
        request_type='post')
    with app.app_context():
        jobs = DB.session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.ADJUSTING_RESOURCES,
                     JobStatus.ADJUSTING_RESOURCES,
                     JobStatus.ADJUSTING_RESOURCES]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))
        DB.session.commit()


def test_nodename_on_fail(db_cfg, simple_workflow_w_errors):
    err_wf = simple_workflow_w_errors
    dag_id = err_wf.task_dag.dag_id

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():

        # Get ERROR job instances
        jobs = DB.session.query(Job).filter_by(dag_id=dag_id).all()
        jobs = [j for j in jobs if j.status == JobStatus.ERROR_FATAL]
        jis = [ji for job in jobs for ji in job.job_instances
               if ji.status == JobInstanceStatus.ERROR]
        DB.session.commit()

        # Make sure all their node names were recorded
        nodenames = [ji.nodename for ji in jis]
        assert nodenames and all(nodenames)


def test_subprocess_return_code_propagation(db_cfg, real_jsm_jqs):
    task = BashTask("not_a_command 1", num_cores=1, m_mem_free='2G',
                    queue="all.q", j_resource=False)

    err_wf = Workflow("my_failing_task", project='ihme_general')
    err_wf.add_task(task)
    err_wf.execute()

    app = db_cfg["app"]
    DB = db_cfg["DB"]

    # get executor_id of row where dag_id matches err_wf's dag_id
    with app.app_context():
        query = (
            f"SELECT executor_id FROM "
            f"job_instance WHERE dag_id = {int(err_wf.dag_id)}")

        errored_job_id = DB.session.execute(query).first()[0]
        DB.session.commit()
    qacct_output = subprocess.check_output(
        args=['qacct', '-j', f'{errored_job_id}'],
        encoding='UTF-8').strip()
    qacct_output = qacct_output.split('\n')[1:]
    qacct_output = {line.split()[0]: line.split()[1] for line in qacct_output}

    exit_status = int(qacct_output['exit_status'])

    # exit status should not be a success, expect failure
    assert exit_status != 0


@pytest.mark.qsubs_jobs
def test_fail_fast(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 1", num_cores=1)
    t2 = BashTask("erroring_out 1", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 10", upstream_tasks=[t1], num_cores=1)
    t4 = BashTask("sleep 11", upstream_tasks=[t3], num_cores=1)
    t5 = BashTask("sleep 12", upstream_tasks=[t4], num_cores=1)

    workflow = Workflow("test_fail_fast", fail_fast=True)
    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.execute()

    assert len(workflow.task_dag.job_list_manager.all_error) == 1
    assert len(workflow.task_dag.job_list_manager.all_done) >= 2


def test_heartbeat(db_cfg, real_jsm_jqs):
    app = db_cfg["app"]
    app.config["SQLALCHEMY_ECHO"] = True
    DB = db_cfg["DB"]

    workflow = Workflow("test_heartbeat")
    workflow._bind()
    workflow._create_workflow_run()

    wfr = workflow.workflow_run

    maxtries = 10
    i = 0
    while i < maxtries:
        i += 1
        with app.app_context():
            row = DB.session.execute(
                "SELECT status FROM workflow_run WHERE id = {}"
                .format(wfr.id)).fetchone()
            DB.session.commit()
            if row[0] == 'R':
                break
        sleep(2)
    if i > maxtries:
        raise Exception("The workflow failed to reconcile in 20 seconds.")

    from jobmon.server.health_monitor.health_monitor import \
        HealthMonitor
    hm = HealthMonitor()
    with app.app_context():

        # This test's workflow should be in the 'active' list
        active_wfrs = hm._get_active_workflow_runs(DB.session)
        assert wfr.id in [w.id for w in active_wfrs]

        # Nothing should be lost since the default (10s) reconciliation heart
        # rate is << than the health monitor's loss_threshold (5min)
        lost = hm._get_lost_workflow_runs(DB.session)
        assert not lost
        DB.session.commit()

    # Setup monitor with a very short loss threshold (~3s = 1min/20)
    hm_hyper = HealthMonitor(loss_threshold=1 / 20.,
                             wf_notification_sink=mock_slack)

    # give some time for the reconciliation to fall behind
    with app.app_context():
        i = 0
        while i < maxtries:
            sleep(10)
            i += 1
            lost = hm_hyper._get_lost_workflow_runs(DB.session)
            DB.session.commit()
            if lost:
                break
        assert lost

        # register the run as lost...
        hm_hyper._register_lost_workflow_runs(lost)

    # ... meaning it should no longer be active... check in a new session
    # to ensure the register-as-lost changes have taken effect
    with app.app_context():
        active = hm_hyper._get_active_workflow_runs(DB.session)
        assert wfr.id not in [w.id for w in active]
        DB.session.commit()

    # Must manully clean this one up because it was not executed.
    cleanup_jlm(workflow)


def test_timeout(real_jsm_jqs, db_cfg):
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


def test_health_monitor_failing_nodes(real_jsm_jqs, db_cfg):
    """Test the Health Montior's identification of failing nodes"""

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

    hm = HealthMonitor(node_notification_sink=mock_slack)
    hm._database = 'singularity'

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
        failing_nodes = hm._calculate_node_failure_rate(DB.session, active_wfrs)
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
        failing_nodes = hm._calculate_node_failure_rate(DB.session, active_wfrs)
        assert 'new_fake_node.ihme.washington.edu' not in failing_nodes


def test_add_tasks_to_workflow(real_jsm_jqs, db_cfg):
    """Make sure adding tasks to a workflow (and not just a task dag) works"""
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


def test_anonymous_workflow(db_cfg, real_jsm_jqs):
    # Make sure uuid is created for an anonymous workflow
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


@pytest.mark.qsubs_jobs
def test_workflow_sge_args(db_cfg, real_jsm_jqs):
    """Test to make sure that the correct information is available to the
     worker cli. For some reason, there are times when this fails and whatever
     executor this workflow is pointing at has a working directory configured,
     but the executor that is actually qsubbing does not have a working
     directory configured. Is this because of the jqs and jsm interfering or a
     problem with the job instance reconcilers not accessing the correct
     executor somehow? """
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


def test_workflow_identical_args(real_jsm_jqs, db_cfg):
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


def test_resume_workflow(real_jsm_jqs, db_cfg):

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
        gently_kill_command(p1.pid)
        return

    # now create an identical workflow which should kill the previous job
    # and workflow process
    workflow = resumable_workflow()
    workflow._bind()
    workflow._create_workflow_run()
    cleanup_jlm(workflow)

    # check if forked process was zombied
    res = subprocess.check_output(f"ps -ax | grep {p1.pid} | grep -v grep",
                                  shell=True, universal_newlines=True)
    assert "Z+" in res
    p1.join()

    # check qstat to make sure jobs isn't pending or running any more.
    # There canbe latency so wait at most 3 minutes for it's state
    # to update in SGE
    max_sleep = 180  # 3 min max till test fails
    slept = 0
    ex_id_list = sge_utils.qstat("pr").keys()
    while executor_id in ex_id_list and slept <= max_sleep:
        sleep(5)
        slept += 5
        ex_id_list = sge_utils.qstat("pr").keys()
    assert executor_id not in ex_id_list


def test_workflow_resource_adjustment(simple_workflow_w_errors, db_cfg):
    workflow = simple_workflow_w_errors

    assert workflow.workflow_run.resource_adjustment == 0.5

    wf_id = workflow.id
    workflow.resource_adjustment = 0.3
    workflow.resume = ResumeStatus.RESUME
    workflow.execute()

    assert workflow.workflow_run.resource_adjustment == 0.3

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """SELECT resource_adjustment
                   FROM workflow_run
                   WHERE workflow_id = {}""".format(wf_id)
        resp = DB.session.execute(query).fetchall()
        DB.session.commit()

    assert len(resp) == 2
    assert resp[0][0] == 0.5
    assert resp[1][0] == 0.3


def test_resource_scaling(real_jsm_jqs, db_cfg, fast_heartbeat):

    from jobmon.client.swarm.executors import ExecutorParameters

    my_wf = Workflow(
        workflow_args="resource starved workflow",
        project="proj_tools",
        resource_adjustment=0.3)  # resources will scale by 50% on failure

    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=60,  # set max runtime to be shorter than task
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


def test_resource_scaling_config(real_jsm_jqs, db_cfg):
    """ non-default resource adjustment overrides individual resource
    adjustments, more resources than default resources get scaled too"""

    name = 'scale_by_wf_resource_scale'
    task = PythonTask(script=sge_utils.true_path("tests/exceed_mem.py"),
                      name=name, max_runtime_seconds=60, num_cores=2,
                      m_mem_free='600M',
                      resource_scales={'num_cores': 0.8, 'm_mem_free': 0.4,
                                       'max_runtime_seconds': 0.7},
                      max_attempts=2)
    wf = Workflow(workflow_args="resource_underrequest_wf",
                  project="proj_tools",
                  resource_adjustment=0.3)
    wf.add_task(task)
    wf.run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        DB.session.commit()
        assert job.executor_parameter_set.m_mem_free == 0.78
        assert job.executor_parameter_set.max_runtime_seconds == 78
        assert job.executor_parameter_set.num_cores == 2


def test_workflow_resume_new_resources(real_jsm_jqs, db_cfg):
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
