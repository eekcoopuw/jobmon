"""tests experimental code for visualization"""

# from jobmon.client import BashTask
# from jobmon.client import Workflow

# from jobmon.client import shared_requester as req
# from jobmon.server.job_visualization_server.job_visualization_server import (
#     _viz_label_mapping)


# def test_job_status(env_var, db_cfg):
#     rc, resp = req.send_request(
#         app_route='/viz/job_status',
#         message={},
#         request_type='get')
#     assert len(resp["job_statuses_dict"]) > 0
#     for job_status in resp["job_statuses_dict"]:
#         assert job_status["label"] in _viz_label_mapping.values()


# def test_foo(env_var, db_cfg):
#     t1 = BashTask("sleep 10", num_cores=1)
#     t2 = BashTask("sleep 5", upstream_tasks=[t1], num_cores=1)
#     workflow = Workflow()
#     workflow.add_tasks([t1, t2])
#     workflow._bind()

#     # we should have the column headers plus 2 tasks
#     rc, resp = req.send_request(
#         app_route=f'/workflow/{workflow.id}/job_display_details',
#         message={},
#         request_type='get')
#     assert len(resp["jobs"]) == 3
#     last_sync = resp["time"]

#     workflow.run()

#     # now each of our jobs should be in D state
#     rc, resp = req.send_request(
#         app_route=f'/workflow/{workflow.id}/job_display_details',
#         message={"last_sync": last_sync},
#         request_type='get')
#     jobs = resp["jobs"]

#     # zero index in responses is column names so ignore
#     for job in jobs[1:]:
#         # first index is job status which should have moved to done
#         assert job[1] == "DONE"

# from jobmon.client import shared_requester as req
# from jobmon.models.task_dag import TaskDagMeta
# from jobmon.models.workflow import Workflow
# from jobmon.models.workflow_status import WorkflowStatus


# def test_get_workflow(env_var, db_cfg):
#     # test the get route on workflow
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]

#     # add a variety of workflows
#     with app.app_context():
#         for status in [WorkflowStatus.CREATED, WorkflowStatus.RUNNING,
#                        WorkflowStatus.STOPPED, WorkflowStatus.ERROR,
#                        WorkflowStatus.DONE]:
#             task_dag = TaskDagMeta()
#             DB.session.add(task_dag)
#             DB.session.commit()
#             workflow = Workflow(
#                 dag_id=task_dag.dag_id,
#                 workflow_args=f"args {status}",
#                 description=f"description {status}",
#                 status=status)
#             DB.session.add(workflow)
#             DB.session.commit()

#     # check that we got 5 workflows back when not filtering
#     rc, resp = req.send_request(
#         app_route='/client/workflow',
#         message={},
#         request_type='get')
#     assert len(resp["workflow_dcts"]) == 5

#     rc, resp = req.send_request(
#         app_route='/client/workflow',
#         message={"status": [WorkflowStatus.RUNNING, WorkflowStatus.STOPPED]},
#         request_type='get')
#     assert len(resp["workflow_dcts"]) == 2
