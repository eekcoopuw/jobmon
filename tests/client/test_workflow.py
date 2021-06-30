# from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.client.tool import Tool
from jobmon.client.workflow_run import WorkflowRun
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import (WorkflowAlreadyComplete, DuplicateNodeArgsError,
                               WorkflowAlreadyExists)

import pytest


@pytest.fixture
def task_template(db_cfg, client_env):
    tool = Tool()
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


def test_wfargs_update(task_template):
    """test that 2 workflows with different names, have different ids and tasks
    """
    tool = Tool()

    # Create identical dags
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="sleep 2", upstream_tasks=[t1])
    t3 = task_template.create_task(arg="sleep 3", upstream_tasks=[t2])

    t4 = task_template.create_task(arg="sleep 1")
    t5 = task_template.create_task(arg="sleep 2", upstream_tasks=[t4])
    t6 = task_template.create_task(arg="sleep 3", upstream_tasks=[t5])

    wfa1 = "v1"
    wf1 = tool.create_workflow(wfa1)
    wf1.add_tasks([t1, t2, t3])
    wf1.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})

    wfa2 = "v2"
    wf2 = tool.create_workflow(wfa2)
    wf2.add_tasks([t4, t5, t6])
    wf2.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})

    # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
    assert wf1.workflow_id != wf2.workflow_id

    # Make sure the second Workflow has a distinct hash
    assert hash(wf1) != hash(wf2)

    # Make sure the second Workflow has a distinct set of Tasks
    wfr1 = WorkflowRun(wf1.workflow_id)
    wfr1.bind(wf1.tasks)
    wfr2 = WorkflowRun(wf2.workflow_id)
    wfr2.bind(wf2.tasks)
    assert not (set([t.task_id for _, t in wf1.tasks.items()]) &
                set([t.task_id for _, t in wf2.tasks.items()]))


def test_attempt_resume_on_complete_workflow(task_template):
    """Should not allow a resume, but should prompt user to create a new
    workflow by modifying the WorkflowArgs (e.g. new version #)
    """
    tool = Tool()

    # Create identical dags
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="sleep 2", upstream_tasks=[t1])

    # initial workflow should run to completion
    wf1 = tool.create_workflow(name="attempt_resume_on_completed")
    wf1.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    wf1.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})
    wfr1 = WorkflowRun(wf1.workflow_id)
    wfr1.bind(wf1.tasks)
    wfr1._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr1._update_status(WorkflowRunStatus.LAUNCHED)
    wfr1._update_status(WorkflowRunStatus.RUNNING)
    wfr1._update_status(WorkflowRunStatus.DONE)

    # second workflow shouldn't be able to start
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="sleep 2", upstream_tasks=[t1])

    # initial workflow should run to completion
    workflow2 = tool.create_workflow(wf1.workflow_args, name="attempt_resume_on_completed")
    workflow2.add_tasks([t1, t2])

    # bind workflow to db and move to done state
    workflow2.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})
    with pytest.raises(WorkflowAlreadyComplete):
        workflow2._create_workflow_run()


def test_workflow_identical_args(task_template):
    """test that 2 workflows with identical arguments can't exist
    simultaneously"""
    tool = Tool()

    # first workflow runs and finishes
    wf1 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 1")
    wf1.add_task(task)
    wf1.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})

    # tries to create an identical workflow without the restart flag
    wf2 = tool.create_workflow(workflow_args="same")
    task = task_template.create_task(arg="sleep 2")
    wf2.add_task(task)
    with pytest.raises(WorkflowAlreadyExists):
        wf2.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})


def test_add_same_node_args_twice(client_env):
    tool = Tool()
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"],
        op_args=[]
    )
    a = tt.create_task(node_arg="a", task_arg="a")
    b = tt.create_task(node_arg="a", task_arg="b")

    workflow = tool.create_workflow()
    workflow.add_task(a)
    with pytest.raises(DuplicateNodeArgsError):
        workflow.add_task(b)


def test_numpy_array_node_args(client_env, db_cfg):
    """Test passing an object (set) that is not JSON serializable to node and task args."""
    from jobmon.client.tool import Tool
    tool = Tool(name="numpy_test_tool")
    workflow = tool.create_workflow(name="numpy_test_wf")
    template = tool.get_task_template(
        template_name="numpy_test_template",
        command_template="echo {node_arg} {task_arg}",
        node_args=["node_arg"],
        task_args=["task_arg"]
    )
    task = template.create_task(
        node_arg={1, 2},
        task_arg={3, 4}
    )
    workflow.add_tasks([task])
    workflow.bind(cluster_name="sequential", compute_resources={"queue": "null.q"})
    assert workflow.workflow_id


# def test_compute_resources(db_cfg, client_env):
#     """Test user passed cluster_resources. Need to test: 1. task with compute resources,
#     no workflow resources 2. task with no compute resources, workflow resources 3. tasks with
#     less resources than workflow"""
#     from jobmon.client.tool import Tool
#     tool = Tool(name="cluster_resource_test")
#     wf_compute_resources = {"sequential": {"num_cores": 2, "mem": "2G",
#                                             "max_runtime_seconds": 10, "queue": "null.q",
#                                             "resource_scales": {"runtime": 0.7}},
#                              "buster": {"mem": "5G"}}
#     workflow_1 = tool.create_workflow(name="compute_resource_1",
#                                       compute_resources=wf_compute_resources)
#     template = tool.get_task_template(
#         template_name="my_template",
#         command_template="echo {node_arg}",
#         node_args=["node_arg"]
#     )
#     task_compute_resource = {"sequential": {"num_cores": 1, "mem": "1G",
#                                             "max_runtime_seconds": 1, "queue": "null.q",
#                                             "resource_scales": {"runtime": 0.5}},
#                              "buster": {"mem": "5G"}}
#     # Use case: Task compute resources, no workflow resources
#     task_1 = template.create_task(name="task_1", node_arg={1},
#                                   compute_resources=task_compute_resource,
#                                   cluster_name="sequential")

#     # Use case: No Task compute resources, inherit from workflow compute resources
#     task_2 = template.create_task(name="task_2", node_arg={2},
#                                   cluster_name="sequential")

#     # TODO: Add test case when we implement partial compute resource dicts
#     # Use case: No Task compute resources, inherit from workflow compute resources
#     # Use case: Minimal task resources, keep task runtime, inherit other resources from wf
#     # task_3 = template.create_task(name="task_3", node_arg={3},
#     #                               compute_resources={"max_runtime_seconds": 8},
#     #                               cluster_name="sequential")

#     workflow_1.add_tasks([task_1, task_2])
#     workflow_1.bind()

#     client_wfr = ClientWorkflowRun(
#         workflow_id=workflow_1.workflow_id,
#         executor_class="Sequential"
#     )
#     client_wfr.bind(workflow_1.tasks, False, workflow_1._chunk_size)

#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     with app.app_context():
#         query = "SELECT requested_resources " \
#                 "FROM task_resources "
#         res = DB.session.execute(query).fetchall()
#         DB.session.commit()
#     assert res[0][0] == f"{task_compute_resource['sequential']}"
#     assert res[1][0] == f"{wf_compute_resources['sequential']}"

