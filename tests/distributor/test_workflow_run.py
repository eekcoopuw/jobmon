import pytest

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.cluster_type.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.requester import Requester


@pytest.mark.skip(reason="Future task to remove cluster from task")
def test_instantiate_queued_tasks(tool, db_cfg, client_env, task_template, array_template):
    """tests that a task can be instantiated and run and log done"""

    task1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="sequential",
                                         compute_resources={"queue": "null.q"})

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([task1])
    workflow.add_array(array1)
    workflow.bind()
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

    assert len(distributor_wfr.registered_task_instances) == 4
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


def test_distributor_launch(tool, db_cfg, client_env, task_template, array_template):
    # Regression to verify non array task launches.
    from jobmon.serializers import SerializeClusterType
    task_1 = task_template.create_task(arg="echo 1", cluster_name="sequential")

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([task_1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)

    single_distributor_task = DistributorTask(task_id=task_1.task_id, name='launch_task',
                                              array_id=None,
                                              command=task_1.command,
                                              requested_resources=task_1.compute_resources,
                                              requester=requester)

    # Move single task to Q state
    _, _ = requester._send_request(
        app_route=f"/task/{task_1.task_id}/queue",
        message={},
        request_type='post'
    )

    dtis_4 = single_distributor_task.register_task_instance(workflow_run_id=wfr.workflow_run_id)

    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )

    distributor = SequentialDistributor()

    single_task_id = distributor_wfr.launch_task_instance(task_instance=dtis_4,
                                                          cluster=distributor)

    # Check that the single non-array task is running, launched or done
    assert get_task_instance_status(db_cfg, dtis_4.task_instance_id) in ["D", "O", "R"]
    assert distributor_wfr.registered_array_task_instances == []


def test_array_distributor_launch(tool, db_cfg, client_env, task_template, array_template):
    from jobmon.serializers import SerializeClusterType
    from jobmon.cluster_type.dummy import DummyDistributor, DummyWorkerNode

    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="dummy",
                                         compute_resources={"queue": "null.q"})

    task_1 = task_template.create_task(arg="echo 1", cluster_name="sequential")

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_array(array1)
    workflow.add_tasks([task_1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_array = DistributorArray(array_id=array1.array_id,
                                         task_resources_id=array1.task_resources.id,
                                         requested_resources=array1.compute_resources,
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
        for t in array1.tasks.values()
    ]

    single_distributor_task = DistributorTask(task_id=task_1.task_id, name='launch_task',
                                              array_id=None,
                                              command=task_1.command,
                                              requested_resources=task_1.compute_resources,
                                              requester=requester)

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks.values()):
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

    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )

    # Register TIs
    dtis_1 = distributor_wfr.register_task_instance(dts[0])
    dtis_2 = distributor_wfr.register_task_instance(dts[1])
    dtis_3 = distributor_wfr.register_task_instance(dts[2])
    dtis_4 = distributor_wfr.register_task_instance(single_distributor_task)

    distributor_array.instantiated_array_task_instance_ids = [dtis_1.task_instance_id,
                                                              dtis_2.task_instance_id]
    #distributor = SequentialDistributor()
    distributor = DummyDistributor()
    array_id = distributor_wfr.launch_array_instance(array=distributor_array,
                                                     cluster=distributor)
    single_task_id = distributor_wfr.launch_task_instance(task_instance=dtis_4,
                                                          cluster=distributor)

    # Assert that step_ids are mapped correctly
    # Since register_task_instance is called sequentially, we can guarantee the
    # taskinstance IDs are sorted already.
    assert (dtis_1.array_step_id, dtis_2.array_step_id) == (1, 2)
    app, DB = db_cfg['app'], db_cfg['DB']
    with app.app_context():

        q = """
        SELECT array_step_id
        FROM task_instance
        WHERE id = {}"""
        res1 = DB.session.execute(q.format(dtis_1.task_instance_id)).one()
        DB.session.commit()

        assert res1.array_step_id == 1

        res2 = DB.session.execute(q.format(dtis_2.task_instance_id)).one()
        DB.session.commit()

        assert res2.array_step_id == 2

    # Dummy cluster complete all submitted tasks.
    # Task 1 will be done
    assert get_task_instance_status(db_cfg, dtis_1.task_instance_id) == "D"
    # Task 2 will be done
    assert get_task_instance_status(db_cfg, dtis_2.task_instance_id) == "D"
    # Task 3 was not updated
    assert get_task_instance_status(db_cfg, dtis_3.task_instance_id) == "I"
    # The registry was cleared out correctly
    assert distributor_array.instantiated_array_task_instance_ids == []
    assert distributor_wfr.registered_array_task_instances == [dtis_3]

    # Check that the single non-array task is running, launched or done
    assert get_task_instance_status(db_cfg, dtis_4.task_instance_id) == "D"

    ti_1_batch_num = get_batch_number(db_cfg, dtis_1.task_instance_id)
    ti_2_batch_num = get_batch_number(db_cfg, dtis_2.task_instance_id)
    ti_3_batch_num = get_batch_number(db_cfg, dtis_3.task_instance_id)
    # Tasks 1 and 2 are associated with the first array batch, should be grouped together
    assert ti_1_batch_num == 0
    assert ti_2_batch_num == 0
    # Task 3 has not been launched yet and should not have a batch number
    assert ti_3_batch_num is None
    # The first result in the array is always the lowest task instance ID
    assert call_get_array_task_instance_id(
        distributor_array.array_id, ti_1_batch_num, client_env) == \
        dtis_1.task_instance_id

    # Add task 3 to the registered queue, and launch

    DummyWorkerNode.STEP_ID = 1
    distributor_array.instantiated_array_task_instance_ids = [dtis_3.task_instance_id]
    distributor_wfr.launch_array_instance(array=distributor_array, cluster=distributor)

    # Check that the worker node is either launched, running, or done
    assert get_task_instance_status(db_cfg, dtis_3.task_instance_id) in ["D", "O", "R"]
    ti_3_batch_num = get_batch_number(db_cfg, dtis_3.task_instance_id)
    # Task 3 belongs to the second array submission, so gets batch number 1 back
    assert ti_3_batch_num == 1
    assert distributor_wfr.registered_array_task_instances == []
    # Assert that the task instance ID can be associated
    assert call_get_array_task_instance_id(
        distributor_array.array_id, ti_3_batch_num, client_env) == \
        dtis_3.task_instance_id


@pytest.mark.parametrize("wf_limit, array_limit, expected_len", [(10_000, 2, 2),
                                                                 (2, 10_000, 2),
                                                                 (2, 3, 2),
                                                                 (3, 2, 2)])
def test_array_concurrency(tool, db_cfg, client_env, array_template, wf_limit, array_limit, expected_len):
    """Use Case 1: Array concurrency limit is set, workflow is not. Array should be limited by
    the array's max_concurrently running value"""
    # Use Case 1: Array concurrency limit is set, workflow concurrency limit not set
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="multiprocess",
                                         compute_resources={"queue": "null.q"},
                                         max_concurrently_running=array_limit)

    workflow_1 = tool.create_workflow(name="test_array_concurrency_1",
                                      max_concurrently_running=wf_limit)
    workflow_1.add_array(array1)
    workflow_1.bind()
    wfr_1 = workflow_1._create_workflow_run()

    requester = Requester(client_env)

    distributor_array = DistributorArray(array_id=array1.array_id,
                                         task_resources_id=array1.task_resources.id,
                                         requested_resources=array1.compute_resources,
                                         name="example_array",
                                         requester=requester,
                                         max_concurrently_running=array_limit
                                         )

    dts = [
        DistributorTask(task_id=t.task_id,
                        array_id=array1.array_id,
                        name='array_ti',
                        command=t.command,
                        requested_resources=t.compute_resources,
                        requester=requester)
        for t in array1.tasks.values()
    ]

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks.values()):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )

    distributor_wfr = DistributorWorkflowRun(
        workflow_1.workflow_id, wfr_1.workflow_run_id, requester
    )

    # Add array to cache
    distributor_wfr.add_new_array(distributor_array)

    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr_1.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr_1.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr_1.workflow_run_id)

    distributor_array.instantiated_array_task_instance_ids = [dtis_1.task_instance_id,
                                                              dtis_2.task_instance_id,
                                                              dtis_3.task_instance_id]

    distributor_wfr.prep_tis_for_launch([dtis_1, dtis_2, dtis_3], wf_limit)

    assert len(distributor_array.prepped_for_launch_array_task_instance_ids) == expected_len

