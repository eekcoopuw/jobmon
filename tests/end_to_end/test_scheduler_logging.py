import logging


def test_scheduler_logging(client_env, caplog):
    """Test to check that scheduler logs are sent to stdout properly."""
    from jobmon.client.api import Tool
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    # Pytest config sets log level at debug, but default in client config is INFO.
    # For a representative test we need to use INFO level.
    caplog.set_level(logging.INFO)

    t = Tool("logging_testing_tool")
    t.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    tt = t.get_task_template(
        template_name="logging_tt", command_template="{command}", node_args=["command"]
    )
    t1 = tt.create_task(command="echo 10")
    workflow = t.create_workflow()
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()
    requester = Requester(client_env)

    # Run the scheduler in a separate process like the workflow does,
    # and check we get the same logs
    workflow._distributor_proc = workflow._start_distributor_service(
        wfr.workflow_run_id, 180
    )
    assert "Instantiating Distributor Process" in caplog.text
    assert "does not implement get_errored_jobs methods" in caplog.text
    caplog.clear()

    swarm = WorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=requester,
    )
    workflow._run_swarm(swarm)
    assert "1 newly completed tasks. 100.0 percent done." in caplog.text
    caplog.clear()

    # Assert that log levels are implemented appropriately - no debug logs by default
    sched_logger = logging.getLogger("jobmon.client.distributor.distributor_service")
    sched_logger.info("Info log")
    sched_logger.debug("Debug log")
    assert "Info log" in caplog.text
    assert "Debug log" not in caplog.text
