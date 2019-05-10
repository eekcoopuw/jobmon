from http import HTTPStatus as StatusCodes
from flask import jsonify, request, Blueprint
import logging


from jobmon.models import DB
from jobmon.models.workflow import Workflow


jvs = Blueprint("job_visualization_server", __name__)


logger = logging.getLogger(__name__)


def get_time(session):
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


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
