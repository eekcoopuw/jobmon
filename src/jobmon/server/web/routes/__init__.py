"""Routes used by task instances on worker nodes."""
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import current_app as app, jsonify
from flask import Blueprint
from jobmon.server.web.models import DB


finite_state_machine = Blueprint("finite_state_machine", __name__)


@finite_state_machine.route('/', methods=['GET'])
def is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    app.logger.info(
        f"{os.getpid()}: {finite_state_machine.__class__.__name__} received is_alive?"
    )
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/time", methods=['GET'])
def get_pst_now() -> Any:
    """Get the current time from the database."""
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/health", methods=['GET'])
def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp


from jobmon.server.web.routes import (dag, node, task, task_instance, task_template, tool,
                                      tool_version, workflow, workflow_run, cluster_type,
                                      cluster, queue)
