"""Routes for Task Resources."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
import structlog

from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.routes.fsm._common import _get_logfile_template


logger = structlog.get_logger(__name__)


@blueprint.route("/task_resources/<task_resources_id>", methods=["GET"])
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    structlog.threadlocal.bind_threadlocal(task_resources_id=task_resources_id)
    data = cast(Dict, request.get_json())

    session = SessionLocal()
    with session.begin():
        template_type = data.get("template_type", "both")
        requested_resources, queue_name = _get_logfile_template(
            task_resources_id,
            template_type,
            session
        )

    resp = jsonify(requested_resources=requested_resources, queue_name=queue_name)
    resp.status_code = StatusCodes.OK
    return resp
