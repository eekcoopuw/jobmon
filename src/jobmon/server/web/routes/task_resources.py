"""Routes for Task Resources."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy.sql import text
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes import finite_state_machine


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route("/task_resources/<task_resources_id>", methods=["GET"])
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    bind_to_logger(task_resources_id=task_resources_id)

    # Check if the array is already bound, if so return it
    task_resources_stmt = """
        SELECT task_resources.*
        FROM task_resources
        WHERE
            task_resources.id = :task_resources_id
    """
    task_resources = (
        DB.session.query(TaskResources)
        .from_statement(text(task_resources_stmt))
        .params(task_resources_id=task_resources_id)
        .one()
    )
    DB.session.commit()

    resp = jsonify(task_resources=task_resources.to_wire_as_task_resources())
    resp.status_code = StatusCodes.OK
    return resp
