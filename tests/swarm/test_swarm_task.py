def test_swarmtask_resources_integration(tool, task_template, db_cfg):
    """Check that taskresources defined in task are passed to swarmtask appropriately"""
    from jobmon.constants import TaskResourcesType, WorkflowRunStatus
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    workflow = tool.create_workflow(default_cluster_name="multiprocess")

    # Create tasks
    task = task_template.create_task(
        arg="echo qux",
        compute_resources={"cores": 10, "queue": "null.q"},
        resource_scales={"cores": 0.5},
        cluster_name="multiprocess",
    )

    # Add to workflow, bind and create wfr
    workflow.add_task(task)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)
    # Check swarmtask resources
    swarmtask = swarm.tasks[task.task_id]
    initial_resources = swarmtask.task_resources
    assert initial_resources.concrete_resources.resources == {
        "cores": 10,
        "queue": "null.q",
    }
    assert initial_resources.task_resources_type_id == TaskResourcesType.ORIGINAL

    # Queue the task. TRs should then be validated
    swarm.queue_task(swarmtask)
    # No change in resource values, so type id stays the same
    assert swarmtask.task_resources.task_resources_type_id == TaskResourcesType.ORIGINAL
    assert id(swarmtask.task_resources) == initial_resources

    # Move task to adjusting
    app, DB = db_cfg['app'], db_cfg['DB']

    with app.app_context():
        sql = """
            UPDATE task
            SET status = :status
            WHERE id = :id
        """
        DB.session.execute(sql, {'status': 'A', 'id': swarmtask.task_id})
        DB.session.commit()

    # Call adjust. Multiprocess doesn't implement adjust, but the path should work
    # and adjust task resources
    swarm.adjust_task(swarmtask)
    scaled_params = swarmtask.task_resources
    # No change in resource values, so type id stays the same
    assert scaled_params.task_resources_type_id == TaskResourcesType.ORIGINAL
    assert id(scaled_params) == id(initial_resources)
    assert scaled_params.concrete_resources.resources == {
        "cores": 10,
        "queue": "null.q",
    }  # No scaling implemented
