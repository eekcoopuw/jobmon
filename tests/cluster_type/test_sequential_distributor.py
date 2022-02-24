from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.requester import Requester


def test_sequential_array(tool, db_cfg, client_env, array_template):

    from jobmon.client.distributor.distributor_task import DistributorTask
    from jobmon.client.distributor.distributor_array import DistributorArray
    from jobmon.client.distributor.distributor_workflow_run import (
        DistributorWorkflowRun,
    )

    array1 = array_template.create_array(
        arg=[1, 2, 3], cluster_name="sequential", compute_resources={"queue": "null.q"}
    )

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")
    workflow.add_array(array1)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)

    distributor_array = DistributorArray(
        array_id=array1.array_id,
        task_resources_id=array1.task_resources.id,
        requested_resources=array1.compute_resources,
        name="example_array",
        requester=requester,
    )

    dts = [
        DistributorTask(
            task_id=t.task_id,
            array_id=array1.array_id,
            name="array_ti",
            command=t.command,
            requested_resources=t.compute_resources,
            requester=requester,
        )
        for t in array1.tasks.values()
    ]

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks.values()):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue", message={}, request_type="post"
        )

    distributor_wfr = DistributorWorkflowRun(
        workflow.workflow_id, wfr.workflow_run_id, requester
    )

    # Add array to cache
    distributor_wfr.add_new_array(distributor_array)

    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr.workflow_run_id)

    seq_distributor = SequentialDistributor()
    command = seq_distributor.build_worker_node_command(
        array_id=array1.array_id, batch_number=1
    )
    distributor_id = seq_distributor.submit_array_to_batch_distributor(
        command=command,
        name="seq_array",
        requested_resources={"queue": "null.q"},
        array_length=3,
    )

    # Ensure all ran successfully
    app, DB = db_cfg["app"], db_cfg["DB"]

    with app.app_context():

        q = """
        SELECT status
        FROM task_instance
        WHERE id IN {}""".format(
            (dtis_1.task_instance_id, dtis_2.task_instance_id, dtis_3.task_instance_id)
        )

        res = DB.session.execute(q).fetchall()
        DB.session.commit()

        statuses = [r.status for r in res]
        assert statuses == ["D"] * 3

    # Sequential distributor _exit_info dict should be populated with 3 values
    expected_exit_info = {f"{distributor_id}.{i}": 0 for i in range(1, 4)}
    assert seq_distributor._exit_info == expected_exit_info
