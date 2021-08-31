"""Routes for Tool Versions."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
import sqlalchemy
from sqlalchemy.sql import text
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.server_side_exception import InvalidUsage


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route('/tool_version', methods=['POST'])
def add_tool_version() -> Any:
    """Add a new version for a Tool."""
    # check input variable
    data = request.get_json()

    try:
        tool_version_id = data.get("tool_version_id")
        tool_id = data.get("tool_id")
        params = {}
        where_clause = []
        if tool_version_id is not None:
            params["id"] = int(tool_version_id)
            where_clause.append("id = :id")
        if tool_id is not None:
            params["tool_id"] = int(tool_id)
            where_clause.append("tool_id = :tool_id")
        if tool_id is None and tool_version_id is None:
            raise ValueError("must specify tool_id or tool_version_id in message")
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    try:
        tool_version = ToolVersion(**params)
        DB.session.add(tool_version)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        where_clause = where_clause.join(" AND ")
        query = """
            SELECT
                tool_version.*
            FROM
                tool_version
            WHERE
                {where_clause}
        """.format(where_clause=where_clause)
        tool_version = DB.session.query(ToolVersion).from_statement(text(query)).params(
            **params
        ).one()

    tool_version = tool_version.to_wire_as_client_tool_version()
    resp = jsonify(tool_version=tool_version)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route('/tool_version/<tool_version_id>/task_templates', methods=['GET'])
def get_task_templates(tool_version_id: int) -> Any:
    """Get the Tool Version."""
    # check input variable
    bind_to_logger(tool_version_id=tool_version_id)
    logger.info("Getting available task_templates")

    # get data from db
    query = """
        SELECT
            task_template.*
        FROM
            task_template
        WHERE
            tool_version_id = :tool_version_id"""
    task_templates = DB.session.query(TaskTemplate).from_statement(text(query)).params(
        tool_version_id=tool_version_id
    ).all()
    DB.session.commit()
    task_templates = [t.to_wire_as_client_task_template() for t in task_templates]
    resp = jsonify(task_templates=task_templates)
    resp.status_code = StatusCodes.OK
    return resp
