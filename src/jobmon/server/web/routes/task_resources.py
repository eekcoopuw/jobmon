"""Routes for Task Resources."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.routes._common import _get_logfile_template

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route("/task_resources/<task_resources_id>", methods=["POST"])
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    bind_to_logger(task_resources_id=task_resources_id)
    data = request.get_json()

    template_type = data.get("template_type", "both")
    requested_resources, queue_name = _get_logfile_template(task_resources_id, template_type)

    resp = jsonify(requested_resources=requested_resources, queue_name=queue_name)
    resp.status_code = StatusCodes.OK
    return resp
