def test_exceed_runtime_task(db_cfg, client_env):
    from subprocess import check_output
    from jobmon.client.api import BashTask
    from jobmon.client.execution.strategies.sge.sge_utils import true_path
    from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.models.task import Task

    name = "over_run_time_task"
    task = BashTask(command="sleep 10", name=name, max_runtime_seconds=5)
    # executor = SGEExecutor(project="proj_scicomp")
    workflow = UnknownWorkflow(executor_class="SGEExecutor")
    workflow.add_tasks([task])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task = DB.session.query(Task).filter_by(name=name).first()
        tid = [ti for ti in task.task_instances][0].executor_id
        resp = check_output(f"qacct -j {tid} | grep 'exit_status\|failed'",
                            shell=True, universal_newlines=True)
        assert ("247" in resp) or ("137" in resp)
        assert task.task_instances[0].status == 'Z'
        assert task.status == 'F'


