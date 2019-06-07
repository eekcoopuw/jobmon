from http import HTTPStatus as StatusCodes
from flask import jsonify, request, Blueprint
import logging


from jobmon.models import DB
from jobmon.models.workflow import Workflow
from jobmon.models.job_status import JobStatus


jvs = Blueprint("job_visualization_server", __name__)


logger = logging.getLogger(__name__)


def get_time(session):
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


_viz_label_mapping = {
    "REGISTERED": "PENDING",
    "QUEUED_FOR_INSTANTIATION": "PENDING",
    "INSTANTIATED": "PENDING",
    "RUNNING": "RUNNING",
    "ERROR_RECOVERABLE": "RECOVERING",
    "ADJUSTING_RESOURCES": "PENDING",
    "ERROR_FATAL": "FATAL",
    "DONE": "DONE",
}


@jvs.route('/job_status', methods=['GET'])
def get_job_statuses():
    """get job status metadata"""
    job_statuses = DB.session.query(JobStatus).all()
    DB.session.commit()
    job_statuses_dict = [js.to_wire() for js in job_statuses]

    # remap to viz names
    for job_status in job_statuses_dict:
        job_status_viz = _viz_label_mapping[job_status["label"]]
        job_status["label"] = job_status_viz

    # send to client
    resp = jsonify(job_statuses_dict=job_statuses_dict)
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow', methods=['GET'])
def get_workflows_by_status():
    """get all workflows with a given status

    Args:
        status (list, None): list of valid statuses from WorkflowStatus. If
            None, then all workflows are returned
    """
    if request.args.get('status', None) is not None:
        workflows = DB.session.query(Workflow)\
            .filter(Workflow.status.in_(request.args.getlist('status')))\
            .all()
    else:
        workflows = DB.session.query(Workflow).all()

    workflow_dcts = [w.to_wire() for w in workflows]
    logger.info("workflow_dcts={}".format(workflow_dcts))
    resp = jsonify(workflow_dcts=workflow_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow/<workflow_id>/job_display_details', methods=['GET'])
def get_job_display_details_by_workflow(workflow_id):
    """Get the jobs that have changed status since a given time for a workflow.

    Args:
        last_sync (datetime, None): get all jobs that have been updated since
            this time. If not given, defaults to '2010-01-01 00:00:00'

    Returns:
        jobs=[{"job_id": int, "status": str, "display_group": (job group)}]
        time=datetime (use in next request to get jobs that have been updated)
    """
    next_sync = get_time(DB.session)
    DB.session.commit()
    last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
    display_vals_query = """
    SELECT
        job.job_id,
        job.status,
        job_attribute.value AS display_group
    FROM
        workflow
    JOIN
        job
            ON workflow.dag_id = job.dag_id
    LEFT JOIN
        job_attribute
            ON job.job_id = job_attribute.job_id AND attribute_type = 18
    WHERE
        workflow.id = :workflow_id
        AND job.status_date >= :last_sync
    ORDER BY job.job_id
    """
    res = DB.session.execute(
        display_vals_query,
        {"workflow_id": workflow_id, "last_sync": last_sync}).fetchall()

    # unpack results, discarding the column name in the row_proxies for smaller
    # payloads
    res = [tuple(data[1] for data in row_proxy.items()) for row_proxy in res]
    res = [("job_id", "status", "display_group")] + res
    resp = jsonify(jobs=res, time=next_sync)
    resp.status_code = StatusCodes.OK
    return resp
