def test_distributor_context(tool, task_template, client_env):
    from jobmon.client.workflow import DistributorContext

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    distributor_context = DistributorContext("sequential", wfr.workflow_run_id, 10)
    distributor_context.__enter__()
    assert distributor_context.alive()

    distributor_context.__exit__()
    assert not distributor_context.alive()
