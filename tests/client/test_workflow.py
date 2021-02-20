import pytest

from jobmon.exceptions import WorkflowAlreadyComplete
from jobmon.constants import WorkflowRunStatus
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.exceptions import DuplicateNodeArgsError


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
    wf1.bind()

    wfa2 = "v2"
    wf2 = UnknownWorkflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.bind()

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
    workflow1.bind()
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
    wf1.bind()

    # tries to create an identical workflow without the restart flag
    wf2 = UnknownWorkflow(workflow_args="same")
    task = BashTask("sleep 2")
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.run()


def test_add_same_node_args_twice(client_env):
    from jobmon.client.tool import Tool
    tool = Tool.create_tool(name="unknown")
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
        op_args=[]
    )
    params = ExecutorParameters(executor_class="DummyExecutor")
    a = tt.create_task(node_arg="a", task_arg="a", executor_parameters=params)
    b = tt.create_task(node_arg="a", task_arg="b", executor_parameters=params)

    workflow = tool.create_workflow()
    workflow.add_task(a)
    with pytest.raises(DuplicateNodeArgsError):
        workflow.add_task(b)


def test_numpy_array_node_args(client_env, db_cfg):
    """Test passing an object (set) that is not JSON serializable to node and task args."""
    from jobmon.client.tool import Tool
    tool = Tool.create_tool(name="numpy_test_tool")
    workflow = tool.create_workflow(name="numpy_test_wf")
    workflow.set_executor(executor_class="SequentialExecutor")
    template = tool.get_task_template(
        template_name="numpy_test_template",
        command_template="echo {node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"]
    )
    executor_parameters = ExecutorParameters(executor_class="SequentialExecutor",
                                             max_runtime_seconds=30)
    task = template.create_task(
        executor_parameters=executor_parameters,
        node_arg={1, 2},
        task_arg={3, 4}
    )
    workflow.add_tasks([task])
    workflow_run = workflow.run()
    assert workflow_run.status == WorkflowRunStatus.DONE
