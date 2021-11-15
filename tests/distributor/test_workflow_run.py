
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.requester import Requester


def test_instantiate_queued_tasks(tool, db_cfg, client_env, task_template, array_template):
    """tests that a task can be instantiated and run and log done"""

    task1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="sequential",
                                         compute_resources={"queue": "null.q"})

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([task1])
    workflow.add_array(array1)
    workflow.bind()
    workflow.bind_arrays()
    wfr = workflow._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )
    tasks = distributor_wfr.get_queued_tasks(4)
    for task in tasks:
        distributor_wfr.register_task_instance(task)

    assert len(distributor_wfr.registered_task_instances) == 1
    assert len(distributor_wfr.registered_array_task_instances) == 3
