import os

from jobmon.requester import Requester


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


class MockDistributorProc:
    def is_alive(self):
        return True


def test_limited_error_log(db_cfg, client_env):
    from jobmon.client.tool import Tool
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor

    tool = Tool()
    wf = tool.create_workflow(name="random_workflow")
    template = tool.get_task_template(
        template_name="some_template",
        command_template="python {node_arg}",
        node_args=["node_arg"],
    )
    task_resources = {
        "num_cores": 1,
        "mem": "1G",
        "max_runtime_seconds": 600,
        "queue": "null.q",
    }
    task = template.create_task(
        name="task1",
        node_arg=os.path.join(thisdir, "fill_pipe.py"),
        compute_resources=task_resources,
        cluster_name="sequential",
        max_attempts=1,
    )
    wf.add_tasks([task])
    wf.bind()
    wfr = wf._create_workflow_run()

    swarm = SwarmWorkflowRun(
        workflow_id=wfr.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(wf.tasks.values()),
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())  # expand the generator

    requester = Requester(client_env)
    distributor = DistributorService(
        wf.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )

    distributor._get_tasks_queued_for_instantiation()
    distributor.distribute()
    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = (
            "SELECT description "
            "FROM task_instance_error_log t1, task_instance t2, workflow_run t3 "
            "WHERE t1.task_instance_id=t2.id "
            "AND t2.workflow_run_id=t3.id "
            "AND t3.workflow_id={}".format(wf.workflow_id)
        )
        res = DB.session.execute(query).fetchone()
        DB.session.commit()

    error = res[0]
    assert error == (("a" * 2 ** 10 + "\n") * (2 ** 8))[-10000:]
