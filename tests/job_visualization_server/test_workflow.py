from jobmon.client import shared_requester as req
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_status import WorkflowStatus


def test_get_workflow(env_var, db_cfg):
    # test the get route on workflow
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    # add a variety of workflows
    with app.app_context():
        for status in [WorkflowStatus.CREATED, WorkflowStatus.RUNNING,
                       WorkflowStatus.STOPPED, WorkflowStatus.ERROR,
                       WorkflowStatus.DONE]:
            task_dag = TaskDagMeta()
            DB.session.add(task_dag)
            DB.session.commit()
            workflow = Workflow(
                dag_id=task_dag.dag_id,
                workflow_args=f"args {status}",
                description=f"description {status}",
                status=status)
            DB.session.add(workflow)
            DB.session.commit()

    # check that we got 5 workflows back when not filtering
    rc, resp = req.send_request(
        app_route='/workflow',
        message={},
        request_type='get')
    assert len(resp["workflow_dcts"]) == 5

    rc, resp = req.send_request(
        app_route='/workflow',
        message={"status": [WorkflowStatus.RUNNING, WorkflowStatus.STOPPED]},
        request_type='get')
    assert len(resp["workflow_dcts"]) == 2
