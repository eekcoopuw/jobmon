from flask import Blueprint, jsonify

from jobmon.server.integration.qpid.maxpss_queue import MaxpssQ

qpid = Blueprint("qpid_integration", __name__)


@qpid.route("/qpid/add_to_maxpss_q/<executor_id>", methods=['POST'])
def add_to_queue(executor_id: int):
    MaxpssQ().put(executor_id)
    resp = jsonify()
    resp.status_code = 200
    return resp
