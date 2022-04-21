from sqlalchemy.sql import text

from jobmon.constants import TaskInstanceStatus


def test_heartbeat_on_launched(tool, db_cfg, client_env, task_template):
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )
    from jobmon.server.web.models.task_instance import TaskInstance

    # create the workflow and bind to database
    t1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 10")
    t2 = tool.active_task_templates["simple_template"].create_task(arg="sleep 11")

    workflow = tool.create_workflow(name="test_instantiate_queued_jobs_on_sequential")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # launch the task then log a heartbeat
    distributor_service = DistributorService(
        MultiprocessDistributor(parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # log a heartbeat. sequential will think it's still running
    distributor_service.log_task_instance_report_by_date()

    # check the heartbeat date is greater than the latest status
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT *
        FROM task_instance
        WHERE task_id in :task_ids
        ORDER BY id"""
        task_instances = (
            DB.session.query(TaskInstance)
            .from_statement(text(sql))
            .params(task_ids=[t1.task_id, t2.task_id])
            .all()
        )
        DB.session.commit()

    for ti in task_instances:
        assert ti.status_date < ti.report_by_date

    distributor_service.cluster.stop()
