import pytest
from time import sleep
import os

from jobmon.database import session_scope
from jobmon.meta_models.task_dag import TaskDagMeta
from jobmon.models import Job, JobInstanceStatus, JobInstance, JobStatus
from jobmon.services.health_monitor import HealthMonitor
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.task_dag import DagExecutionStatus, TaskDag
from jobmon.workflow.workflow import Workflow, WorkflowDAO, WorkflowStatus, \
    WorkflowAlreadyComplete
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


@pytest.fixture(scope='function')
def second_dag(db_cfg, jsm_jqs, request):
    """Use a fxiture for dag creation so that the dags' JobInstanceFactories
    and JobInstanceReconcilers get cleaned up after each test"""
    dag = TaskDag(name=request.node.name + "2", interrupt_on_error=False)
    yield dag
    if dag.job_list_manager:
        dag.job_list_manager.disconnect()


@pytest.fixture
def simple_workflow(dag):
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)
    workflow.execute()
    return workflow


@pytest.fixture
def simple_workflow_w_errors(dag):
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


def test_wfargs_update(dag, second_dag):
    # Create identical dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = dag
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 1")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 3", upstream_tasks=[t5])
    dag2 = second_dag
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
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


def test_dag_update(dag, second_dag):
    # Create different dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = dag
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 3")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 1", upstream_tasks=[t5])
    dag2 = second_dag
    dag2.add_tasks([t4, t5, t6])

    wfa1 = "dag_update"
    wf1 = Workflow(dag1, wfa1)
    wf1.execute()

    wfa2 = "dag_update"
    wf2 = Workflow(dag2, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


def test_wfagrs_dag_update(dag, second_dag):
    # Create different dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = dag
    dag1.add_tasks([t1, t2, t3])

    t4 = BashTask("sleep 3")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 1", upstream_tasks=[t5])
    dag2 = second_dag
    dag2.add_tasks([t4, t5, t6])

    wfa1 = "wfargs_dag_update"
    wf1 = Workflow(dag1, wfa1)
    wf1.execute()

    wfa2 = "wfargs_dag_update"
    wf2 = Workflow(dag2, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert wf1.hash != wf2.hash

    # Make sure the second Workflow has a distinct set of Jobs
    assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
                set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


def test_stop_resume(simple_workflow, tmpdir, second_dag):
    # Manually modify the database so that some mid-dag jobs appear in
    # a running / non-complete / non-error state
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.bound_tasks.items()]

    to_run_jid = job_ids[-1]
    with session_scope() as session:
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
    dag = second_dag
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
    with session_scope() as session:
        wf_run = (session.query(WorkflowRunDAO).filter_by(
            id=stopped_wf.workflow_run.id).first())
        assert wf_run.status == WorkflowRunStatus.STOPPED
        wf_run_jobs = session.query(JobInstance).filter_by(
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


def test_reset_attempts_on_resume(simple_workflow, second_dag):
    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts
    stopped_wf = simple_workflow
    job_ids = [t.job_id for _, t in stopped_wf.task_dag.bound_tasks.items()]

    mod_jid = job_ids[1]

    with session_scope() as session:
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
    dag = second_dag
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)

    # Before actually executing the DAG, validate that the database has
    # reset the attempt counters to 0 and the ERROR states to INSTANTIATED
    workflow._bind()
    bt2 = dag.job_list_manager.bound_task_from_task(t2)
    assert bt2.job_id == mod_jid  # Should be bound to stopped-run ID values

    with session_scope() as session:
        jobDAO = session.query(Job).filter_by(job_id=bt2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 0
        assert jobDAO.status == JobStatus.REGISTERED

    workflow.execute()

    # TODO: Check that the user is prompted that they want to resume...

    # Validate that the database indicates the Dag and its Jobs are complete
    with session_scope() as session:
        jobDAO = session.query(Job).filter_by(job_id=bt2.job_id).first()
        assert jobDAO.max_attempts == 3
        assert jobDAO.num_attempts == 1
        assert jobDAO.status == JobStatus.DONE

    # Validate that a new WorkflowRun was created and is DONE
    assert workflow.workflow_run.id != stopped_wf.workflow_run.id
    with session_scope() as session:
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


def test_attempt_resume_on_complete_workflow(simple_workflow, second_dag):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)"""

    stopped_wf = simple_workflow

    # Re-create the dag "from scratch" (copy simple_workflow fixture)
    dag = second_dag
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = "my_simple_dag"
    workflow = Workflow(dag, wfa)

    with pytest.raises(WorkflowAlreadyComplete):
        workflow.execute()


def test_new_workflow_existing_dag(dag, second_dag):
    # Should allow creation of the Workflow, and should also create a new DAG.
    dag_nowf = dag
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    dag_nowf.add_tasks([t1, t2])
    dag_nowf._execute()

    # Need to ensure that the Workflow doesn't attach itself to the old DAG.
    dag_wf = second_dag
    t3 = BashTask("sleep 1")
    t4 = BashTask("sleep 2", upstream_tasks=[t3])
    dag_wf.add_tasks([t3, t4])

    wfa = "new_workflow_existing_dag"
    workflow = Workflow(dag_wf, wfa)
    workflow.execute()

    assert workflow.task_dag.dag_id != dag_nowf.dag_id

    # This will mean that the hash of the new DAG matches that of the old DAG,
    # but has a new dag_id
    with session_scope() as session:
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

    with session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.ERROR_FATAL,
                     JobStatus.ERROR_FATAL, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))

    # Now RESET and make sure all the jobs that aren't "DONE" flip back to
    # REGISTERED
    jsm.reset_incomplete_jobs(dag_id)
    with session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        assert len(jobs) == 4

        xstatuses = [JobStatus.DONE, JobStatus.REGISTERED,
                     JobStatus.REGISTERED, JobStatus.REGISTERED]
        assert (sorted([j.status for j in jobs]) ==
                sorted(xstatuses))


def test_nodename_on_fail(simple_workflow_w_errors):

    err_wf = simple_workflow_w_errors
    dag_id = err_wf.task_dag.dag_id

    with session_scope() as session:

        # Get ERROR job instances
        jobs = session.query(Job).filter_by(dag_id=dag_id).all()
        jobs = [j for j in jobs if j.status == JobStatus.ERROR_FATAL]
        jis = [ji for job in jobs for ji in job.job_instances
               if ji.status == JobInstanceStatus.ERROR]

        # Make sure all their node names were recorded
        nodenames = [ji.nodename for ji in jis]
        assert nodenames and all(nodenames)


def test_heartbeat(dag):

    # TODO: Fix this awful hack... I believe the DAG fixtures above create
    # reconcilers that will run for the duration of this module (since they
    # are module level fixtures)... These will mess with the timings of
    # our fresh heartbeat dag we're testing in this function. To get around it,
    # these dummy dags will increment the ID of our dag-of-interest to
    # avoid the timing collisions
    with session_scope() as session:
        for _ in range(5):
            session.add(TaskDagMeta())
        session.commit()

    # ... now let's check out heartbeats
    workflow = Workflow(dag, "test_heartbeat")
    workflow._bind()
    workflow._create_workflow_run()

    wfr = workflow.workflow_run

    # give some time to make sure the dag's reconciliation process
    # has actually started
    sleep(20)

    hm = HealthMonitor()
    with session_scope() as session:

        # This test's workflow should be in the 'active' list
        active_wfrs = hm._get_active_workflow_runs(session)
        assert wfr.id in [w.id for w in active_wfrs]

        # Nothing should be lost since the default (10s) reconciliation heart
        # rate is << than the health monitor's loss_threshold (5min)
        lost = hm._get_lost_workflow_runs(session)
        assert not lost

    # Setup monitor with a very short loss threshold (~3s = 1min/20)
    hm_hyper = HealthMonitor(loss_threshold=1 / 20.,
                             wf_notification_sink=mock_slack)
    with session_scope() as session:

        # the reconciliation heart rate is now > this monitor's threshold,
        # so should be identified as lost
        lost = hm_hyper._get_lost_workflow_runs(session)
        assert lost

        # register the run as lost...
        hm_hyper._register_lost_workflow_runs(lost)

    # ... meaning it should no longer be active... check in a new session
    # to ensure the register-as-lost changes have taken effect
    with session_scope() as session:
        active = hm_hyper._get_active_workflow_runs(session)
        assert wfr.id not in [w.id for w in active]


def test_failing_nodes(dag):

    # these dummy dags will increment the ID of our dag-of-interest to
    # avoid the timing collisions
    with session_scope() as session:
        for _ in range(5):
            session.add(TaskDagMeta())
        session.commit()

    t1 = BashTask("echo 'hello'")
    t2 = BashTask("echo 'to'", upstream_tasks=[t1])
    t3 = BashTask("echo 'the'", upstream_tasks=[t2])
    t4 = BashTask("echo 'beautiful'", upstream_tasks=[t3])
    t5 = BashTask("echo 'world'", upstream_tasks=[t4])
    t6 = BashTask("sleep 1", upstream_tasks=[t5])
    dag.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow = Workflow(dag, "test_failing_nodes")
    workflow.run()

    wfr = workflow.workflow_run

    # give some time to make sure the dag's reconciliation process
    # has actually started
    sleep(20)

    hm = HealthMonitor(node_notification_sink=mock_slack)
    hm._database = 'singularity'
    with session_scope() as session:

        # Manually make the workflow run look like it's still running
        session.execute("""
            UPDATE workflow_run
            SET status='{s}'
            WHERE workflow_id={id}""".format(s=WorkflowRunStatus.RUNNING,
                                             id=workflow.id))

        # This test's workflow should be in the 'active' AND succeeding list
        active_wfrs = hm._get_succeeding_active_workflow_runs(session)
        assert wfr.id in active_wfrs

        # Manually make 5 job instances land on the same node & have them fail
        session.execute("""
            UPDATE job_instance
            SET nodename='fake_node.ihme.washington.edu', status="{s}"
            WHERE job_instance_id < 7 and workflow_run_id={wfr_id}
            """.format(s=JobInstanceStatus.ERROR, wfr_id=wfr.id))
        failing_nodes = hm._calculate_node_failure_rate(session, active_wfrs)
        assert 'fake_node.ihme.washington.edu' in failing_nodes

        # Manually make those job instances land on the same node and have
        # them fail BUT also manually make there dates be older than an hour
        # Ensure they they don't come up because of the time window
        session.execute("""
            UPDATE job_instance
            SET nodename='new_fake_node.ihme.washington.edu', status="{s}",
            status_date = '2018-05-16 17:17:54'
            WHERE job_instance_id < 7 and workflow_run_id={wfr_id}
            """.format(s=JobInstanceStatus.ERROR, wfr_id=wfr.id))
        failing_nodes = hm._calculate_node_failure_rate(session, active_wfrs)
        assert 'new_fake_node.ihme.washington.edu' not in failing_nodes


def test_add_tasks_to_workflow(db_cfg, jsm_jqs):
    """Make sure adding tasks to a workflow (and not just a task dag) works"""
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    wfa = "add_tasks_to_workflow"
    workflow = Workflow(workflow_args=wfa)
    workflow.add_tasks([t1, t2, t3])
    workflow.run()

    with session_scope() as session:
        w = session.query(WorkflowDAO).filter_by(id=workflow.id).first()
        assert w.status == 'D'
        j = session.query(Job).filter_by(dag_id=workflow.task_dag.dag_id).all()
        assert all(t.status == 'D' for t in j)


def test_anonymous_workflow(db_cfg, jsm_jqs):
    # Make sure uuid is created for an anonymous workflow
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    workflow = Workflow()
    workflow.add_tasks([t1, t2, t3])
    workflow.run()

    bt3 = workflow.task_dag.job_list_manager.bound_task_from_task(t3)

    assert workflow.workflow_args is not None

    # Manually flip one of the jobs to Failed
    with session_scope() as session:
        session.execute("""
            UPDATE workflow_run
            SET status='E'
            WHERE workflow_id={id}""".format(id=workflow.id))
        session.execute("""
            UPDATE workflow
            SET status='E'
            WHERE id={id}""".format(id=workflow.id))
        session.execute("""
            UPDATE job
            SET status='F'
            WHERE job_id={id}""".format(id=bt3.job_id))

    # Restart it using the uuid.
    uu_id = workflow.workflow_args
    new_workflow = Workflow(workflow_args=uu_id)
    new_workflow.add_tasks([t1, t2, t3])
    new_workflow.run()

    # Make sure it's the same workflow
    assert workflow.id == new_workflow.id


def test_workflow_status_dates(simple_workflow):
    """Make sure the workflow status dates actually get updated"""
    wfid = simple_workflow.wf_dao.id
    with session_scope() as session:
        wf_dao = session.query(WorkflowDAO).filter_by(id=wfid).first()
        assert wf_dao.status_date != wf_dao.created_date

        wf_runs = wf_dao.workflow_runs
        for wfr in wf_runs:
            assert wfr.created_date != wfr.status_date


def test_workflow_sge_args(db_cfg, jsm_jqs):
    t1 = PythonTask(script='{}/executor_args_check.py'.format(
        os.path.dirname(os.path.realpath(__file__))))
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    wfa = "my_simple_dag"
    workflow = Workflow(workflow_args=wfa, project='proj_jenkins',
                        working_dir='/ihme/centralcomp/auto_test_data',
                        stderr='/ihme/centralcomp/auto_test_data',
                        stdout='/ihme/centralcomp/auto_test_data')
    workflow.add_tasks([t1, t2, t3])
    wf_status = workflow.execute()
    assert wf_status == DagExecutionStatus.SUCCEEDED

    assert workflow.workflow_run.project == 'proj_jenkins'
    assert workflow.workflow_run.working_dir == (
        '/ihme/centralcomp/auto_test_data')
    assert workflow.workflow_run.stderr == '/ihme/centralcomp/auto_test_data'
    assert workflow.workflow_run.stdout == '/ihme/centralcomp/auto_test_data'
