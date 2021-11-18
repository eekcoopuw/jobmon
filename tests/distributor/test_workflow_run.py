
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
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


def test_array_distributor_launch(tool, db_cfg, client_env, task_template, array_template):
    # stuff
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="sequential",
                                         compute_resources={"queue": "null.q"})

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_array(array1)
    workflow.bind()
    workflow.bind_arrays()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_array = DistributorArray(array1.array_id,
                                         array1.task_resources.id,
                                         array1.default_compute_resources_set,
                                         requester
                                         )

    dts = [
        DistributorTask(task_id=t.task_id,
                        array_id=array1.array_id,
                        name='array_ti',
                        command=t.command,
                        requested_resources=t.compute_resources,
                        requester=requester)
        for t in array1.tasks
    ]

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )

    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr.workflow_run_id)

    distributor_array.registered_array_task_instance_ids = [dtis_1, dtis_2]
    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )

    distributor = SequentialDistributor()
    array_id = distributor_wfr.launch_array_instance(array=distributor_array,
                                                     cluster=distributor)
    assert distributor_wfr._triaging_queue == []

    # assert all expected task instances in status "O"

    # assert all excluded task instances in status "I"
    # assert registered_task_instances == []
    # assert all expected task instances have array_batch_num 0
    # assert /get_array_task_instances returns len(expected task instances) values for batch 0

    distributor_array.registered_task_instances = excluded task instances
    # assert distributor_array.batch_number == 1
    # assert no task instances in state I
    # assert registered_task_instances == []
    # assert all excluded task instances have array batch num 1
    # assert /get_array_task_instances returns len(expected task instances) values for batch 1
