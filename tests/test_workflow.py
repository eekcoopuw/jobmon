import pytest
from time import sleep
import os
import uuid
import subprocess

from jobmon import BashTask  # testing new style imports
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
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus


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

    workflow = Workflow("my_failing_args", interrupt_on_error=False,
                        project='ihme_general')
    workflow.add_tasks([t1, t2, t3, t4])
    workflow.execute()
    return workflow


def mock_slack(msg, channel):
    print("{} to be posted to channel: {}".format(msg, channel))


def test_wf_with_stata_temp_dir(real_jsm_jqs, db_cfg):
    t1 = StataTask(script='di "hello"', slots=1)
    t2 = StataTask(script='di "world"', upstream_tasks=[t1], slots=1)

    wf = Workflow("stata_temp_dir_test", interrupt_on_error=False)
    wf.add_tasks([t1, t2])

    success = wf.run()
    assert success


@pytest.mark.qsubs_jobs
def test_wfargs_update(real_jsm_jqs, db_cfg):
    # Create identical dags
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    t4 = BashTask("sleep 1", slots=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], slots=1)
    t6 = BashTask("sleep 3", upstream_tasks=[t5], slots=1)

    wfa1 = "v1"
    wf1 = Workflow(wfa1, interrupt_on_error=False)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "v2"
    wf2 = Workflow(wfa2, interrupt_on_error=False)
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
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    t4 = BashTask("sleep 3", slots=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], slots=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], slots=1)

    wfa1 = "dag_update"
    wf1 = Workflow(wfa1, interrupt_on_error=False)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "dag_update"
    wf2 = Workflow(wfa2, interrupt_on_error=False)
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
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    t4 = BashTask("sleep 3", slots=1)
    t5 = BashTask("sleep 2", upstream_tasks=[t4], slots=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], slots=1)

    wfa1 = "wfargs_dag_update"
    wf1 = Workflow(wfa1, interrupt_on_error=False)
    wf1.add_tasks([t1, t2, t3])
    wf1.execute()

    wfa2 = "wfargs_dag_update"
    wf2 = Workflow(wfa2, interrupt_on_error=False)
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
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    wfa = "my_simple_dag"
    elogdir = str(tmpdir.mkdir("wf_elogs"))
    ologdir = str(tmpdir.mkdir("wf_ologs"))

    workflow = Workflow(wfa, stderr=elogdir, stdout=ologdir,
                        project='proj_jenkins', resume=ResumeStatus.RESUME)
    workflow.add_tasks([t1, t2, t3])
    workflow.execute()

    # TODO: FIGURE OUT WHETHER IT's SENSIBLE TO CLEAR THE done/error
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
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    wfa = "my_simple_dag"
    workflow = Workflow(wfa, interrupt_on_error=False,
                        resume=ResumeStatus.RESUME)
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

    workflow.execute()

    # TODO: Check that the user is prompted that they want to resume...

    # Validate that the database indicates the Dag and its Jobs are complete
    with app.app_context():
        jobDAO = DB.session.query(Job).filter_by(job_id=bt2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 1
        assert jobDAO.status == JobStatus.DONE

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


@pytest.mark.qsubs_jobs
def test_attempt_resume_on_complete_workflow(simple_workflow):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    wfa = "my_simple_dag"
    workflow = Workflow(wfa, interrupt_on_error=False,
                        resume=ResumeStatus.RESUME)
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

        # Make sure all their node names were recorded
        nodenames = [ji.nodename for ji in jis]
        assert nodenames and all(nodenames)


def test_subprocess_return_code_propagation(db_cfg, real_jsm_jqs):
    task = BashTask("not_a_command 1", num_cores=1, m_mem_free='2G',
                    queue="all.q", j_resource=False)

    err_wf = Workflow("my_failing_task", interrupt_on_error=False,
                      project='ihme_general')
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
    qacct_output = subprocess.check_output(
        args=['qacct', '-j', f'{errored_job_id}'],
        encoding='UTF-8').strip()
    qacct_output = qacct_output.split('\n')[1:]
    qacct_output = {line.split()[0]: line.split()[1] for line in qacct_output}

    exit_status = int(qacct_output['exit_status'])

    # exit status should not be a success, expect failure
    assert exit_status != 0


# @pytest.mark.skip(reason="there isn't any guarantee that this fails in time")
@pytest.mark.qsubs_jobs
def test_fail_fast(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("erroring_out 1", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 10", upstream_tasks=[t1], slots=1)
    t4 = BashTask("sleep 11", upstream_tasks=[t3], slots=1)
    t5 = BashTask("sleep 12", upstream_tasks=[t4], slots=1)

    workflow = Workflow("test_fail_fast", fail_fast=True,
                        interrupt_on_error=False)
    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.execute()

    assert len(workflow.task_dag.job_list_manager.all_error) == 1
    assert len(workflow.task_dag.job_list_manager.all_done) == 2


def test_heartbeat(db_cfg, real_jsm_jqs):

    # TODO: Fix this awful hack... I believe the DAG fixtures above create
    # reconcilers that will run for the duration of this module (since they
    # are module level fixtures)... These will mess with the timings of
    # our fresh heartbeat dag we're testing in this function. To get around it,
    # these dummy dags will increment the ID of our dag-of-interest to
    # avoid the timing collisions
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        for _ in range(5):
            DB.session.add(TaskDagMeta())
        DB.session.commit()

    # ... now let's check out heartbeats
    workflow = Workflow("test_heartbeat", interrupt_on_error=False)
    workflow._bind()
    workflow._create_workflow_run()

    wfr = workflow.workflow_run

    # give some time to make sure the dag's reconciliation process
    # has actually started
    sleep(20)

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

    # Setup monitor with a very short loss threshold (~3s = 1min/20)
    hm_hyper = HealthMonitor(loss_threshold=1 / 20.,
                             wf_notification_sink=mock_slack)

    # give some time for the reconciliation to fall behind
    sleep(10)
    with app.app_context():

        # the reconciliation heart rate is now > this monitor's threshold,
        # so should be identified as lost
        lost = hm_hyper._get_lost_workflow_runs(DB.session)
        if not lost:
            sleep(50)
            lost = hm_hyper._get_lost_workflow_runs(DB.session)
        assert lost

        # register the run as lost...
        hm_hyper._register_lost_workflow_runs(lost)

    # ... meaning it should no longer be active... check in a new session
    # to ensure the register-as-lost changes have taken effect
    with app.app_context():
        active = hm_hyper._get_active_workflow_runs(DB.session)
        assert wfr.id not in [w.id for w in active]


def test_timeout(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 10", slots=1)
    t2 = BashTask("sleep 11", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 12", upstream_tasks=[t2], slots=1)

    wfa1 = "timeout_dag"
    wf1 = Workflow(wfa1, interrupt_on_error=False, seconds_until_timeout=3)
    wf1.add_tasks([t1, t2, t3])

    with pytest.raises(RuntimeError) as error:
        wf1.execute()

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (3 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_failing_nodes(real_jsm_jqs, db_cfg):

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

    t1 = BashTask("echo 'hello'", slots=1)
    t2 = BashTask("echo 'to'", upstream_tasks=[t1], slots=1)
    t3 = BashTask("echo 'the'", upstream_tasks=[t2], slots=1)
    t4 = BashTask("echo 'beautiful'", upstream_tasks=[t3], slots=1)
    t5 = BashTask("echo 'world'", upstream_tasks=[t4], slots=1)
    t6 = BashTask("sleep 1", upstream_tasks=[t5], slots=1)
    workflow = Workflow("test_failing_nodes", interrupt_on_error=False)
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.run()

    wfr = workflow.workflow_run

    # give some time to make sure the dag's reconciliation process
    # has actually started
    sleep(20)

    hm = HealthMonitor(node_notification_sink=mock_slack)
    hm._database = 'singularity'

    with app.app_context():

        # Manually make the workflow run look like it's still running
        DB.session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.RUNNING,
                                             id=workflow.id))

        # This test's workflow should be in the 'active' AND succeeding list
        active_wfrs = hm._get_succeeding_active_workflow_runs(DB.session)
        assert wfr.id in active_wfrs

        # Manually make 5 job instances land on the same node & have them fail
        DB.session.execute("""
            UPDATE job_instance
            SET nodename='fake_node.ihme.washington.edu', status="{s}"
            WHERE job_instance_id < 7 and workflow_run_id={wfr_id}
            """.format(s=JobInstanceStatus.ERROR, wfr_id=wfr.id))
        failing_nodes = hm._calculate_node_failure_rate(DB.session, active_wfrs)
        assert 'fake_node.ihme.washington.edu' in failing_nodes

        # Manually make those job instances land on the same node and have
        # them fail BUT also manually make there dates be older than an hour
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
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    wfa = "add_tasks_to_workflow"
    workflow = Workflow(workflow_args=wfa, interrupt_on_error=False)
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


def test_anonymous_workflow(db_cfg, real_jsm_jqs):
    # Make sure uuid is created for an anonymous workflow
    t1 = BashTask("sleep 1", slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)

    workflow = Workflow(interrupt_on_error=False)
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
    new_workflow = Workflow(workflow_args=uu_id, interrupt_on_error=False,
                            resume=True)
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


@pytest.mark.qsubs_jobs
def test_workflow_sge_args(real_jsm_jqs, db_cfg):
    t1 = PythonTask(script='{}/executor_args_check.py'.format(
        os.path.dirname(os.path.realpath(__file__))), slots=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], slots=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], slots=1)


    wfa = "my_simple_dag"
    workflow = Workflow(workflow_args=wfa, project='proj_jenkins',
                        working_dir='/ihme/centralcomp/auto_test_data',
                        stderr='/tmp',
                        stdout='/tmp')
    workflow.add_tasks([t1, t2, t3])
    wf_status = workflow.execute()

    assert wf_status == DagExecutionStatus.SUCCEEDED
    # If the working directory is not set correctly then executor_args_check.py
    # will fail and write its error message into the job_instance_error_log
    # table

    assert workflow.workflow_run.project == 'proj_jenkins'
    assert workflow.workflow_run.working_dir == (
        '/ihme/centralcomp/auto_test_data')
    assert workflow.workflow_run.stderr == '/tmp'
    assert workflow.workflow_run.stdout == '/tmp'
    assert workflow.workflow_run.executor_class == 'SGEExecutor'


def test_workflow_identical_args(real_jsm_jqs, db_cfg, monkeypatch):
    # first workflow runs and finishes
    wf1 = Workflow(workflow_args="same", project='proj_jenkins')
    task = BashTask("sleep 2", slots=1)
    wf1.add_task(task)
    wf1.execute()

    # tries to create an identical workflow without the restart flag
    wf2 = Workflow(workflow_args="same", project='proj_jenkins')
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.execute()

    # creates a workflow, okayed to restart, but original workflow is done
    wf3 = Workflow(workflow_args="same", project='proj_jenkins',
                   resume=ResumeStatus.RESUME)
    wf3.add_task(task)
    with pytest.raises(WorkflowAlreadyComplete):
        wf3.execute()

