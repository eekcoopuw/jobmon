import os
import pytest
import subprocess
import uuid
from time import sleep

from jobmon.client import BashTask
from jobmon.client import StataTask
from jobmon.client import Workflow
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client import shared_requester as req
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    ResumeStatus
from tests.conftest import teardown_db
import tests.workflow_utils as wu


path_to_file = os.path.dirname(__file__)


def test_wf_with_stata_temp_dir(db_cfg, env_var):
    teardown_db(db_cfg)
    t1 = StataTask(script='di "hello"', num_cores=1)
    t2 = StataTask(script='di "world"', upstream_tasks=[t1], num_cores=1)

    wf = Workflow("stata_temp_dir_test")
    wf.add_tasks([t1, t2])

    success = wf.run()
    assert success
    teardown_db(db_cfg)


@pytest.mark.qsubs_jobs
def test_wfargs_update(db_cfg, env_var):
    teardown_db(db_cfg)
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
    teardown_db(db_cfg)


@pytest.mark.qsubs_jobs
def test_resource_arguments(db_cfg, env_var):
    """
    Test the parsing/serialization max run time and cores.
    90,000 seconds is deliberately longer than one day, testing a specific
    bug"""
    teardown_db(db_cfg)
    t1 = BashTask("sleep 10",
                  queue='all.q',
                  max_runtime_seconds=90_000,
                  num_cores=2)
    wf = Workflow("test_resource_arguments-{}".format(uuid.uuid4()))
    wf.add_tasks([t1])
    return_code = wf.execute()
    assert return_code == DagExecutionStatus.SUCCEEDED
    teardown_db(db_cfg)


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
            WHERE job_id={jid}""".format(s=JobStatus.REGISTERED,
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
            WHERE job_id={jid}""".format(s=JobStatus.REGISTERED,
                                         jid=to_run_jid))
        DB.session.commit()

    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    t1 = BashTask("sleep 1", num_cores=1, m_mem_free='1G')
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1, m_mem_free='1G')
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1, m_mem_free='1G')

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
    teardown_db(db_cfg)


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
        assert jobDAO.status == JobStatus.REGISTERED
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
    teardown_db(db_cfg)


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
                     JobStatus.ERROR_FATAL, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))
        DB.session.commit()

    # Now RESET and make sure all the jobs that aren't "DONE" flip back to
    # REGISTERED
    rc, _ = req.send_request(
        app_route='/task_dag/{}/reset_incomplete_jobs'.format(dag_id),
        message={},
        request_type='post')
    with app.app_context():
        jobs = DB.session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.REGISTERED,
                     JobStatus.REGISTERED, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))
        DB.session.commit()
    teardown_db(db_cfg)


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
    teardown_db(db_cfg)


def test_subprocess_return_code_propagation(db_cfg, env_var):
    teardown_db(db_cfg)
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
    teardown_db(db_cfg)


@pytest.mark.qsubs_jobs
def test_fail_fast(db_cfg, env_var):
    teardown_db(db_cfg)
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
    teardown_db(db_cfg)


def test_heartbeat(db_cfg, env_var, ephemera):
    teardown_db(db_cfg)
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
    hm = HealthMonitor(app=app)
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
                             wf_notification_sink=wu.mock_slack,
                             app=app)

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
    wu.cleanup_jlm(workflow)
    teardown_db(db_cfg)
