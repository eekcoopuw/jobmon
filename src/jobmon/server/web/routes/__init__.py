"""Routes used by task instances on worker nodes."""
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import jsonify
from flask import Blueprint
from structlog import get_logger

from jobmon.server.web.database import SessionLocal

finite_state_machine = Blueprint("finite_state_machine", __name__)

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = get_logger(__name__)


# ############################ LANDING ROUTES ################################################
@finite_state_machine.route("/", methods=["GET"])
def is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    logger.info(
        f"{os.getpid()}: {finite_state_machine.__class__.__name__} received is_alive?"
    )
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


def _get_time() -> str:
    with SessionLocal.begin() as session:
        time = session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
        session.commit()
    time = time["time"]
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


@finite_state_machine.route("/time", methods=["GET"])
def get_pst_now() -> Any:
    """Get the time from the database."""
    time = _get_time()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/health", methods=["GET"])
def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    _get_time()
    resp = jsonify(status="OK")
    resp.status_code = StatusCodes.OK
    return resp


# ############################ TESTING ROUTES ################################################
@finite_state_machine.route("/test_bad", methods=["GET"])
def test_bad_route():
    """Test route to force a 500 error."""
    with SessionLocal.begin() as session:
        session.execute("SELECT * FROM blip_bloop_table").all()
        session.commit()


# ############################ APPLICATION ROUTES #############################################
# from jobmon.server.web.routes import (
#     array,
#     dag,
#     node,
#     task,
#     task_instance,
#     task_resources,
#     task_template,
#     tool,
#     tool_version,
#     workflow,
#     workflow_run,
#     cluster_type,
#     cluster,
#     queue,
# )
