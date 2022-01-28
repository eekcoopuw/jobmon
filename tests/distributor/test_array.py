
def test_record_array_batch_num(db_cfg, client_env, tool):
    from jobmon.client.distributor.distributor_array import DistributorArray
    from jobmon.client.distributor.distributor_task import DistributorTask
    from jobmon.requester import Requester

    tt = tool.get_task_template(
        template_name="whatever",
        command_template="{command} {task_arg} {narg1}",
        node_args=["narg1"],
        task_args=["task_arg"],
        op_args=["command"],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )

    array = tt.create_array(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        compute_resources={"queue": "null.q"}
    )

    assert len(array.tasks) == 3  # Created on init
    wf = tool.create_workflow()
    wf.add_array(array)
    wf.bind()
    #wf.bind_arrays()
    wfr = wf._create_workflow_run()
    requester = Requester(client_env)
    distributor_array = DistributorArray(array_id=array.array_id,
                                         task_resources_id=array.task_resources.id,
                                         requested_resources=array.compute_resources,
                                         name="example_array",
                                         requester=requester
                                         )
    dts = [
        DistributorTask(task_id=t.task_id,
                        array_id=array.array_id,
                        name='array_ti',
                        command=t.command,
                        requested_resources=t.compute_resources,
                        requester=requester)
        for t in array.tasks.values()
    ]
    # Move all tasks to Q state
    for tid in (t.task_id for t in array.tasks.values()):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )
    # Register TIs
    dtis_1 = dts[0].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_2 = dts[1].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    dtis_3 = dts[2].register_task_instance(workflow_run_id=wfr.workflow_run_id)
    _, _ = requester.send_request(
        app_route="/task_instance/record_array_batch_num/1",
        message={'task_instance_ids': [dtis_1.task_instance_id, dtis_2.task_instance_id, dtis_3.task_instance_id]},
        request_type='post'
    )
    sorted_tiids = [dtis_1.task_instance_id, dtis_2.task_instance_id, dtis_3.task_instance_id]
    sorted_tiids.sort()
    app, DB = db_cfg["app"], db_cfg["DB"]
    with app.app_context():
        for i in range(len(sorted_tiids)):
            sql = f"""SELECT array_step_id
                FROM task_instance
                WHERE id = {sorted_tiids[i]}"""
            step_id = DB.session.execute(sql).fetchone()["array_step_id"]
            assert step_id == i + 1
