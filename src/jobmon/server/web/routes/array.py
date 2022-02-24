"""Routes for Arrays."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy import text
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.array import Array
from jobmon.server.web.routes import finite_state_machine


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route("/array", methods=["POST"])
def add_array() -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = request.get_json()
    bind_to_logger(
        task_template_version_id=data["task_template_version_id"],
        workflow_id=data["workflow_id"],
    )

    # Check if the array is already bound, if so return it
    array_stmt = """
        SELECT array.*
        FROM array
        WHERE
            workflow_id = :workflow_id
        AND
            task_template_version_id = :task_template_version_id"""
    array = (
        DB.session.query(Array)
        .from_statement(text(array_stmt))
        .params(
            workflow_id=data["workflow_id"],
            task_template_version_id=data["task_template_version_id"],
        )
        .one_or_none()
    )
    DB.session.commit()

    if array is None:  # not found, so need to add it
        array = Array(
            task_template_version_id=data["task_template_version_id"],
            workflow_id=data["workflow_id"],
            max_concurrently_running=data["max_concurrently_running"],
        )
        DB.session.add(array)
    else:
        array.max_concurrently_running = data["max_concurrently_running"]
    DB.session.commit()

    # return result
    resp = jsonify(array_id=array.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/array/<array_id>", methods=["GET"])
def get_array(array_id: int) -> Any:
    """Return an array.

    If not found, bind the array.
    """
    bind_to_logger(array_id=array_id)

    # Check if the array is already bound, if so return it
    array_stmt = """
        SELECT array.*
        FROM array
        WHERE
            array.id = :array_id
    """
    array = (
        DB.session.query(Array)
        .from_statement(text(array_stmt))
        .params(array_id=array_id)
        .one()
    )
    DB.session.commit()

    resp = jsonify(array=array.to_wire_as_distributor_array())
    resp.status_code = StatusCodes.OK
    return resp
