"""Routes for Task Resources."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy import select
import structlog

from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint


logger = structlog.get_logger(__name__)


@blueprint.route("/task_resources/<task_resources_id>", methods=["GET"])
def get_task_resources(task_resources_id: int) -> Any:
    """Return an task_resources."""
    structlog.threadlocal.bind_threadlocal(task_resources_id=task_resources_id)

    with SessionLocal.begin() as session:
        select_stmt = select(TaskResources).where(TaskResources.id == task_resources_id)
        task_resources = session.execute(select_stmt).scalars().one()

    resp = jsonify(task_resources=task_resources.to_wire_as_task_resources())
    resp.status_code = StatusCodes.OK
    return resp
