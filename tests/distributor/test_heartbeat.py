from multiprocessing import Queue

from jobmon.exceptions import ResumeSet

import pytest


@pytest.fixture
def tool(db_cfg, client_env):
    from jobmon.client.tool import Tool

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


class MockDistributorProc:
    def is_alive(self):
        return True


def test_heartbeat(tool, db_cfg, client_env, task_template):
    """test that the TaskInstanceDistributor logs a heartbeat in the database"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="my_beating_heart")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    distributor_service.heartbeat()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT workflow_run.heartbeat_date > CURRENT_TIMESTAMP()
        FROM workflow_run
        WHERE workflow_run.id = :workflow_run_id"""
        res = DB.session.execute(
            sql, {"workflow_run_id": wfr.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res[0] == 1


def test_heartbeat_raises_error(tool, db_cfg, client_env, task_template):
    """test that a heartbeat logged after resume will raise ResumeSet"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="my_heartbeat_error")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    with pytest.raises(ResumeSet):
        distributor_service.heartbeat()


def test_heartbeat_propagate_error(tool, db_cfg, client_env, task_template):
    """test that a heartbeat logged after resume will raise ResumeSet through
    the message queue and can be re_raised"""

    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="heartbeat_propagate_error")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    q = Queue()
    distributor_service.run_distributor(status_queue=q)
    assert q.get() == "ALIVE"

    with pytest.raises(ResumeSet):
        q.get().re_raise()
