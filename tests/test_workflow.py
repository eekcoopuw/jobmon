import pytest

from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow


def test_simple(db_cfg, jsm_jqs):
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = ""
    workflow = Workflow(dag, wfa)
    workflow.execute()


def test_wfargs_update():
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag = TaskDag()
    dag.add_tasks([t1, t2, t3])

    wfa1 = "v1"
    wf1 = Workflow(dag, wfa1)
    wf1.execute()

    wfa2 = "v2"
    wf2 = Workflow(dag, wfa2)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert False

    # Make sure the second Workflow has a distinct set of Jobs and JobInstances
    assert False


def test_dag_update():
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = TaskDag()
    dag1.add_tasks([t1, t2])

    dag2 = TaskDag()
    dag2.add_tasks([t1, t2, t3])

    wfa = ""
    wf1 = Workflow(dag1, wfa)
    wf1.execute()

    wfa = ""
    wf2 = Workflow(dag2, wfa)
    wf2.execute()

    # Make sure the second Workflow has a distinct Workflow ID and WorkflowRun
    # ID
    assert wf1.id != wf2.id

    # Make sure the second Workflow has a distinct hash
    assert False

    # Make sure the second Workflow has a distinct set of Jobs and JobInstances
    assert False


def test_wfagrs_dag_update():
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    dag1 = TaskDag()
    dag1.add_tasks([t1, t2])

    dag2 = TaskDag()
    dag2.add_tasks([t1, t2, t3])

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
    assert False

    # Make sure the second Workflow has a distinct set of Jobs and JobInstances
    assert False


def test_stop_resume():
    # Run a dag

    # Manually modify the database so that some mid-dag jobs appear in
    # a running / non-complete / non-error state

    # Validate that the database has been modified

    # Re-start the dag

    # Check that the user is prompted that they indeed want to resume...

    # Validate that the database indicates the Dag and its Jobs are complete

    # Validate that a new WorkflowRun was created
    pass


def test_reset_attempts_on_resume():
    # Run a dag

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts

    # Validate that the database has been modified

    # Re-instantiate the DAG + Workflow

    # Check that the user is prompted that they want to resume...

    # Before actually executing the DAG, validate that the database has
    # reset the attempt counters to 0 and the ERROR states to INSTANTIATED

    # Validate that the database indicates the Dag and its Jobs are complete

    # Validate that a new WorkflowRun was created
    pass


def test_resume_dag_with_errors():
    # Redundant with "test_reset_attempts_on_resume"??
    pass


def test_attempt_resume_on_complete_workflow():
    # Should not allow a resume, but should prompt user to create a new
    # workflow by modifying the WorkflowArgs (e.g. new version #)
    pass


def test_new_workflow_existing_dag():
    # Should allow creation of the Workflow, and should also create a new DAG.

    # Need to ensure that the Workflow doesn't attach itself to the old DAG.

    # This will mean that the hash of the new DAG matches that of the old DAG,
    # but has a new dag_id
    pass


def test_force_new_workflow_instead_of_resume():
    # Run a dag

    # Manually modify the database so that some mid-dag jobs appear in
    # error state, max-ing out the attempts

    # Validate that the database has been modified

    # Re-instantiate the DAG + Workflow

    # Check that the user is prompted that they want to resume...

    # Choose to 'create new' Workflow instead of resume...

    # Validate that the old Workflow stays as-is, and that a new Workflow is
    # created that runs to completion
    pass
