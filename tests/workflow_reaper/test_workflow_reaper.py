def test_workflow_start(db_cfg, client_env):
    from jobmon.server.health_monitor.workflow_reaper import WorkflowReaper
    from jobmon.client.swarm import workflow_run
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    task = BashTask("sleep 10")
    workflow = UnknownWorkflow("my_simple_dag",
                               executor_class="SequentialExecutor")

    workflow.add_tasks([task])
    workflow._bind()
    wfr = workflow._create_workflow_run()

    wfr.update_status("R")

    reaper = WorkflowReaper()
    result = reaper._get_active_workflow_runs()
    import pdb
    pdb.set_trace()
    assert result == "Hello"
