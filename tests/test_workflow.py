import pytest
from time import sleep

from jobmon import database
from jobmon.meta_models.task_dag import TaskDagMeta
from jobmon.models import Job, JobInstanceStatus, JobStatus
from jobmon.services.health_monitor import HealthMonitor
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow, WorkflowDAO, WorkflowStatus, \
    WorkflowAlreadyComplete
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


@pytest.fixture
def simple_workflow(db_cfg, jsm_jqs):
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)
    workflow.execute()
    return workflow


@pytest.fixture
def simple_workflow_w_errors(db_cfg, jsm_jqs):
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("not_a_command 1", upstream_tasks=[t1])
    t3 = BashTask("sleep 30", upstream_tasks=[t1], max_runtime=1)
    t4 = BashTask("not_a_command 3", upstream_tasks=[t2, t3])
    dag.add_tasks([t1, t2, t3, t4])

    workflow = Workflow(dag, "my_failing_args")
    workflow.execute()
    return workflow


def mock_slack(msg, channel):
    print("{} to be posted to channel: {}".format(msg, channel))


def test_wfargs_update(db_cfg, jsm_jqs):
    # Create identical dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = TaskDag()
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 1")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 3", upstream_tasks=[t5])
    dag2 = TaskDag()
    dag2.add_tasks([t4, t5, t6])

    wfa1 = "v1"
    wf1 = Workflow(dag1, wfa1)
    wf1.execute()

    wfa2 = "v2"
    wf2 = Workflow(dag2, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.tasks.items()]))


def test_dag_update(db_cfg, jsm_jqs):
    # Create different dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = TaskDag()
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 3")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 1", upstream_tasks=[t5])
    dag2 = TaskDag()
    dag2.add_tasks([t4, t5, t6])

    wfa1 = "v1"
    wf1 = Workflow(dag1, wfa1)
    wf1.execute()

    wfa2 = "v1"
    wf2 = Workflow(dag2, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.tasks.items()]))


def test_wfagrs_dag_update(db_cfg, jsm_jqs):
    # Create different dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = TaskDag()
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 3")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 1", upstream_tasks=[t5])
    dag2 = TaskDag()
    dag2.add_tasks([t4, t5, t6])

    wfa1 = "v1"
    wf1 = Workflow(dag1, wfa1)
    wf1.execute()

    wfa2 = "v2"
    wf2 = Workflow(dag2, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.tasks.items()]))


def test_stop_resume(simple_workflow, tmpdir):
    # Manually modify the database so that some mid-dag jobs appear in
    # a running / non-complete / non-error state
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.tasks.items()]

    to_run_jid = job_ids[-1]
    with database.session_scope() as session:
        session.execute("""
            UPDATE job
            SET status='{s}'
            WHERE job_id={jid}""".format(s=JobStatus.REGISTERED,
                                         jid=to_run_jid))
        session.execute("""
            UPDATE workflow
            SET status='{s}'
            WHERE id={id}""".format(s=WorkflowStatus.STOPPED,
                                             id=stopped_wf.id))
        session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.STOPPED,
                                             id=stopped_wf.id))
        session.execute("""
            DELETE FROM job_instance
            WHERE job_id={jid}""".format(s=JobStatus.REGISTERED,
                                         jid=to_run_jid))

    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    elogdir = str(tmpdir.mkdir("wf_elogs"))
    ologdir = str(tmpdir.mkdir("wf_ologs"))
    workflow = Workflow(dag, wfa, stderr=elogdir, stdout=ologdir,
                        project='proj_jenkins')
    workflow.execute()

    # Check that finished tasks aren't added to the top fringe
    assert workflow.task_dag.top_fringe == [t3]

    # TODO: Check that the user is prompted that they indeed want to resume...

    # Validate that the new workflow has the same ID as the 'stopped' one
    assert workflow.id == stopped_wf.id

    # Validate that a new WorkflowRun was created
    assert workflow.workflow_run.id != stopped_wf.workflow_run.id

    # Validate that a new WorkflowRun has different logdirs and project
    assert workflow.workflow_run.stderr != stopped_wf.workflow_run.stderr
    assert workflow.workflow_run.stdout != stopped_wf.workflow_run.stdout
    assert workflow.workflow_run.project != stopped_wf.workflow_run.project

    # Validate that the database indicates the Dag and its Jobs are complete
    assert workflow.status == WorkflowStatus.DONE

    for _, task in workflow.task_dag.tasks.items():
        assert task.status == JobStatus.DONE


def test_reset_attempts_on_resume(simple_workflow):
    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.tasks.items()]

    mod_jid = job_ids[1]

    with database.session_scope() as session:
        session.execute("""
            UPDATE job
            SET status='{s}', num_attempts=3, max_attempts=3
            WHERE job_id={jid}""".format(s=JobStatus.ERROR_FATAL,
                                         jid=mod_jid))
        session.execute("""
            UPDATE job_instance
            SET status='{s}'
            WHERE job_id={jid}""".format(s=JobInstanceStatus.ERROR,
                                         jid=mod_jid))
        session.execute("""
            UPDATE workflow
            SET status='{s}'
            WHERE id={id}""".format(s=WorkflowStatus.ERROR,
                                             id=stopped_wf.id))
        session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.ERROR,
                                             id=stopped_wf.id))

    # Re-instantiate the DAG + Workflow
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)

    # Before actually executing the DAG, validate that the database has
    # reset the attempt counters to 0 and the ERROR states to INSTANTIATED
    workflow._bind()
    assert t2.job_id == mod_jid  # Should be bound to stopped-run ID values

    with database.session_scope() as session:
        jobDAO = session.query(Job).filter_by(job_id=t2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 0
        assert jobDAO.status == JobStatus.REGISTERED

    workflow.execute()

    # TODO: Check that the user is prompted that they want to resume...

    # Validate that the database indicates the Dag and its Jobs are complete
    with database.session_scope() as session:
        jobDAO = session.query(Job).filter_by(job_id=t2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 1
        assert jobDAO.status == JobStatus.DONE

    # Validate that a new WorkflowRun was created and is DONE
    assert workflow.workflow_run.id != stopped_wf.workflow_run.id
    with database.session_scope() as session:
        wfDAO = session.query(WorkflowDAO).filter_by(id=workflow.id).first()
        assert wfDAO.status == WorkflowStatus.DONE

        wfrDAOs = session.query(WorkflowRunDAO).filter_by(
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


def test_attempt_resume_on_complete_workflow(simple_workflow):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)"""

    stopped_wf = simple_workflow

    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)

    with pytest.raises(WorkflowAlreadyComplete):
        workflow.execute()


def test_new_workflow_existing_dag(db_cfg, jsm_jqs):
    # Should allow creation of the Workflow, and should also create a new DAG.
    dag_nowf = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    dag_nowf.add_tasks([t1, t2])
    dag_nowf.execute()

    # Need to ensure that the Workflow doesn't attach itself to the old DAG.
    dag_wf = TaskDag()
    t3 = BashTask("sleep 1")
    t4 = BashTask("sleep 2", upstream_tasks=[t3])
    dag_wf.add_tasks([t3, t4])

    wfa = "my_simple_dag"
    workflow = Workflow(dag_wf, wfa)
    workflow.execute()

    assert workflow.task_dag.dag_id != dag_nowf.dag_id

    # This will mean that the hash of the new DAG matches that of the old DAG,
    # but has a new dag_id
    with database.session_scope() as session:
        nowf_tdms = session.query(TaskDagMeta).filter_by(
            dag_id=dag_nowf.dag_id).all()
        wf_tdms = session.query(TaskDagMeta).filter_by(
            dag_id=workflow.task_dag.dag_id).all()

        assert len(nowf_tdms) == 1
        assert len(wf_tdms) == 1
        assert wf_tdms[0].dag_hash == nowf_tdms[0].dag_hash


def test_force_new_workflow_instead_of_resume(simple_workflow):
    # TODO (design): Is there ever a scenario where this is a good thing to do?
    # This is more or less possible by updating WorkflowArgs... which I think
    # is better practice than trying to create a new Workflow with identical
    # args and DAG, which is a violation of our current concept of Workflow
    # uniqueness. If we really want to all this behavior, we need to further
    # refine that concept and potentially add another piece of information
    # to the Workflow hash itself.
    pass


def test_dag_reset(jsm_jqs, simple_workflow_w_errors):
    # Alias to shorter names...
    jsm, _ = jsm_jqs
    err_wf  = simple_workflow_w_errors

    dag_id = err_wf.task_dag.dag_id

    with database.session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.ERROR_FATAL,
                     JobStatus.ERROR_FATAL, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))

    # Now RESET and make sure all the jobs that aren't "DONE" flip back to
    # REGISTERED
    jsm.reset_incomplete_jobs(dag_id)
    with database.session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.REGISTERED,
                     JobStatus.REGISTERED, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))


def test_nodename_on_fail(simple_workflow_w_errors):

    err_wf = simple_workflow_w_errors
    dag_id = err_wf.task_dag.dag_id

    with database.session_scope() as session:

        # Get ERROR job instances
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        jobs = [j for j in jobs if j.status == JobStatus.ERROR_FATAL]
        jis = [ji for job in jobs for ji in job.job_instances
               if ji.status == JobInstanceStatus.ERROR]

        # Make sure all their node names were recorded
        nodenames = [ji.nodename for ji in jis]
        assert nodenames and all(nodenames)


def test_heartbeat(db_cfg, jsm_jqs):

    # TODO: Fix this awful hack... I believe the DAG fixtures above create
    # reconcilers that will run for the duration of this module (since they
    # are module level fixtures)... These will mess with the timings of
    # our fresh heartbeat dag we're testing in this function. To get around it,
    # these dummy dags will increment the ID of our dag-of-interest to
    # avoid the timing collisions
    with database.session_scope() as session:
        for _ in range(5):
            session.add(TaskDagMeta())
        session.commit()

    # ... now let's check out heartbeats
    dag = TaskDag()
    workflow = Workflow(dag, "test_heartbeat")
    workflow._bind()
    workflow._create_workflow_run()

    wfr = workflow.workflow_run

    # give some time to make sure the dag's reconciliation process
    # has actually started
    sleep(20)

    hm = HealthMonitor()
    with database.session_scope() as session:

        # This test's workflow should be in the 'active' list
        active_wfrs = hm._get_active_workflow_runs(session)
        assert wfr.id in [w.id for w in active_wfrs]

        # Nothing should be lost since the default (10s) reconciliation heart
        # rate is << than the health monitor's loss_threshold (5min)
        lost = hm._get_lost_workflow_runs(session)
        assert not lost


    # Setup monitor with a very short loss threshold (~3s = 1min/20)
    hm_hyper = HealthMonitor(loss_threshold=1/20.,
                             notification_sink=mock_slack)
    with database.session_scope() as session:

        # the reconciliation heart rate is now > this monitor's threshold,
        # so should be identified as lost
        lost = hm_hyper._get_lost_workflow_runs(session)
        assert lost

        # register the run as lost...
        hm_hyper._register_lost_workflow_runs(lost)

    # ... meaning it should no longer be active... check in a new session
    # to ensure the register-as-lost changes have taken effect
    with database.session_scope() as session:
        active = hm_hyper._get_active_workflow_runs(session)
        assert wfr.id not in [w.id for w in active]
