from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
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


def get_task_instance_status(db_cfg, task_instance_id):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = f"SELECT status FROM task_instance WHERE id = {task_instance_id}"
        resp = DB.session.execute(query).fetchone()[0]
    return resp


def get_batch_number(db_cfg, task_instance_id):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = f"SELECT array_batch_num FROM task_instance WHERE id = {task_instance_id}"
        resp = DB.session.execute(query).fetchone()[0]
    return resp


def call_get_array_task_instance_id(array_id, batch_num, client_env):
    app_route = f"/get_array_task_instance_id/{array_id}/{batch_num}/1"
    requester = Requester(client_env)
    rc, resp = requester.send_request(
        app_route=app_route,
        message={},
        request_type='get'
    )
    return resp['task_instance_id']


def test_array_distributor_launch(tool, db_cfg, client_env, task_template, array_template):
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="sequential",
                                         compute_resources={"queue": "null.q"})

    task_1 = task_template.create_task(arg="echo 1", cluster_name="sequential")

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_array(array1)
    workflow.add_tasks([task_1])
    workflow.bind()
    workflow.bind_arrays()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_array = DistributorArray(array_id=array1.array_id,
                                         task_resources_id=array1.task_resources.id,
                                         requested_resources=array1.default_compute_resources_set,
                                         name="example_array",
                                         requester=requester
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

    single_distributor_task = DistributorTask(task_id=task_1.task_id, name='launch_task',
                                              array_id=None,
                                              command=task_1.command,
                                              requested_resources=task_1.compute_resources,
                                              requester=requester)

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )

    # Move single task to Q state
    _, _ = requester._send_request(
        app_route=f"/task/{task_1.task_id}/queue",
        message={},
        request_type='post'
    )

    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_4 = single_distributor_task.register_task_instance(workflow_run_id=wfr.workflow_run_id)

    distributor_array.instantiated_array_task_instance_ids = [dtis_1.task_instance_id,
                                                              dtis_2.task_instance_id]
    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )

    distributor = SequentialDistributor()
    array_id = distributor_wfr.launch_array_instance(array=distributor_array,
                                                     cluster=distributor)
    single_task_id = distributor_wfr.launch_task_instance(task_instance=dtis_4,
                                                          cluster=distributor)

    # Sequential distributor will submit only the first task in an array.
    # Task 1 will be running, launched, or done
    assert get_task_instance_status(db_cfg, dtis_1.task_instance_id) in ["D", "O", "R"]
    # Task 2 was moved to launched
    assert get_task_instance_status(db_cfg, dtis_2.task_instance_id) == "O"
    # Task 3 was not updated
    assert get_task_instance_status(db_cfg, dtis_3.task_instance_id) == "I"
    # The registry was cleared out correctly
    assert distributor_wfr.registered_array_task_instances == []

    # Check that the single non-array task is running, launched or done
    assert get_task_instance_status(db_cfg, dtis_4.task_instance_id) in ["D", "O", "R"]

    ti_1_batch_num = get_batch_number(db_cfg, dtis_1.task_instance_id)
    ti_2_batch_num = get_batch_number(db_cfg, dtis_2.task_instance_id)
    ti_3_batch_num = get_batch_number(db_cfg, dtis_3.task_instance_id)
    # Tasks 1 and 2 are associated with the first array batch, should be grouped together
    assert ti_1_batch_num == 0
    assert ti_2_batch_num == 0
    # Task 3 has not been launched yet and should not have a batch number
    assert ti_3_batch_num is None
    # The first result in the array is always the lowest task instance ID
    assert call_get_array_task_instance_id(array_id, ti_1_batch_num, client_env) == \
           dtis_1.task_instance_id

    # Add task 3 to the registered queue, and launch
    distributor_array.instantiated_array_task_instance_ids = [dtis_3.task_instance_id]
    distributor_wfr.launch_array_instance(array=distributor_array, cluster=distributor)

    # Check that the worker node is either launched, running, or done
    assert get_task_instance_status(db_cfg, dtis_3.task_instance_id) in ["D", "O", "R"]
    ti_3_batch_num = get_batch_number(db_cfg, dtis_3.task_instance_id)
    # Task 3 belongs to the second array submission, so gets batch number 1 back
    assert ti_3_batch_num == 1
    assert distributor_wfr.registered_array_task_instances == []
    # Assert that the task instance ID can be associated
    assert call_get_array_task_instance_id(array_id, ti_3_batch_num, client_env) == \
           dtis_3.task_instance_id


def test_array_concurrency(tool, db_cfg, client_env, array_template, task_template):
    # Use Case 1: Array concurrency limit is set, workflow concurrency limit not set
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"},
                                         max_concurrently_running=2)

    workflow_1 = tool.create_workflow(name="test_array_concurrency_1")
    workflow_1.add_array(array1)
    workflow_1.bind()
    workflow_1.bind_arrays()
    wfr_1 = workflow_1._create_workflow_run()

    # Use Case 2: Array concurrency limit is not set, workflow concurrency limit is set
    array2 = array_template.create_array(arg=[4, 5, 6], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"})
    workflow_2 = tool.create_workflow(name="test_array_concurrency_2",
                                      max_concurrently_running=2)
    workflow_2.add_array(array2)
    workflow_2.bind()
    workflow_2.bind_arrays()
    wfr_2 = workflow_2._create_workflow_run()

    # Use Case 3: Array concurrency limit is set (higher rate than wf), workflow concurrency
    # limit is also set (lower rate than array). TODO: EITHER RAISE VALUE ERROR OR CHANGE THE
    # ARRAY MAX TO BE THE SAME AS WORKFLOW
    array3 = array_template.create_array(arg=[7, 8, 9], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"},
                                         max_concurrently_running=3)
    workflow_3 = tool.create_workflow(name="test_array_concurrency_2",
                                      max_concurrently_running=1)
    workflow_3.add_array(array2)
    workflow_3.bind()
    workflow_3.bind_arrays()
    wfr_3 = workflow_3._create_workflow_run()

    # Use Case 4: Array concurrency limit is set (lower rate than wf), workflow concurrency
    # limit is also set (higher rate than array). Should submit one job at a time.
    array4 = array_template.create_array(arg=[10, 11, 12], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"},
                                         max_concurrently_running=1)
    workflow_4 = tool.create_workflow(name="test_array_concurrency_2",
                                      max_concurrently_running=3)
    workflow_4.add_array(array4)
    workflow_4.bind()
    workflow_4.bind_arrays()
    wfr_4 = workflow_4._create_workflow_run()

    # Use case 5: Concurrency limit set on both wf and array objects, other tasks are added to
    # wf besides array.
    array5 = array_template.create_array(arg=[13, 14, 15], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"},
                                         max_concurrently_running=1)
    workflow_5 = tool.create_workflow(name="test_array_concurrency_2",
                                      max_concurrently_running=3)
    task_1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    task_2 = task_template.create_task(arg="echo 2", cluster_name="sequential")
    task_3 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow_5.add_array(array5)
    workflow_5.add_tasks([task_1, task_2, task_3])
    workflow_5.bind()
    workflow_5.bind_arrays()
    wfr_5 = workflow_5._create_workflow_run()

    # Use case 6: Boundary limit - add 0 or negative test

    # TODO: CREATE A DISTRIBUTOR SERVICE, CALL THE LAUNCH METHODS AND MAKE SURE WHAT'S COMING
    # BACK IS EXPECTED
    # TODO: HAVE LAUNCH METHODS RETURN THE TASKS THEY SUBMITTED

    assert "hello" == "hi"
