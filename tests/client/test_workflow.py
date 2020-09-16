import pytest

from jobmon.exceptions import WorkflowAlreadyComplete
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.workflow import Workflow


def test_get_chunk():
    wf = Workflow(tool_version_id=1, chunk_size=10)
    assert wf._get_chunk(10, 2) is None
    assert wf._get_chunk(10, 1) == (0, 9)
    assert wf._get_chunk(20, 2) == (10, 19)
    assert wf._get_chunk(19, 2) == (10, 18)
    assert wf._get_chunk(8, 1)  == (0, 7)


def test_wfargs_update(client_env, db_cfg):
    """test that 2 workflows with different names, have different ids and tasks
    """
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow

    # Create identical dags
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])

    t4 = BashTask("sleep 1")
    t5 = BashTask("sleep 2", upstream_tasks=[t4])
    t6 = BashTask("sleep 3", upstream_tasks=[t5])

    wfa1 = "v1"
    wf1 = UnknownWorkflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1._bind(False)

    wfa2 = "v2"
    wf2 = UnknownWorkflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2._bind(False)

    # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
    assert wf1.workflow_id != wf2.workflow_id

    # Make sure the second Workflow has a distinct hash
    assert hash(wf1) != hash(wf2)

    # Make sure the second Workflow has a distinct set of Tasks
    wf1._create_workflow_run()
    wf2._create_workflow_run()
    assert not (set([t.task_id for _, t in wf1.tasks.items()]) &
                set([t.task_id for _, t in wf2.tasks.items()]))


def test_attempt_resume_on_complete_workflow(client_env, db_cfg):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow

    # initial workflow should run to completion
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    wf_args = "my_simple_workflow"
    workflow1 = UnknownWorkflow(wf_args, name="attempt_resume_on_completed",
                                executor_class="SequentialExecutor")
    workflow1.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    workflow1._bind()
    wfr = workflow1._create_workflow_run()
    wfr.update_status(WorkflowRunStatus.RUNNING)
    wfr.update_status(WorkflowRunStatus.DONE)

    # second workflow shouldn't be able to start
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])

    workflow2 = UnknownWorkflow(wf_args, name="attempt_resume_on_completed",
                                executor_class="SequentialExecutor")
    workflow2.add_tasks([t1, t2])
    # workflow2.set_executor(SequentialExecutor())
    with pytest.raises(WorkflowAlreadyComplete):
        workflow2.run()


def test_workflow_identical_args(client_env, db_cfg):
    """test that 2 workflows with identical arguments can't exist
    simultaneously"""
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.exceptions import WorkflowAlreadyExists

    # first workflow runs and finishes
    wf1 = UnknownWorkflow(workflow_args="same")
    task = BashTask("sleep 2")
    wf1.add_task(task)
    wf1._bind(False)

    # tries to create an identical workflow without the restart flag
    wf2 = UnknownWorkflow(workflow_args="same")
    task = BashTask("sleep 2")
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.run()
