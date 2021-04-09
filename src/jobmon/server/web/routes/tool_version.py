"""Routes for Tool Versions"""
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.server_side_exception import InvalidUsage

from sqlalchemy.sql import text

from . import jobmon_client


@jobmon_client.route('/tool_version', methods=['POST'])
def add_tool_version():
    """Add a new version for a Tool."""
    # check input variable
    data = request.get_json()
    try:
        tool_version_id = int(data["tool_version_id"])
        app.logger = app.logger.bind(tool_version_id=tool_version_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    tool_version = ToolVersion(tool_version_id=tool_version_id)
    DB.session.add(tool_version)
    DB.session.commit()
    tool_version = tool_version.to_wire_as_client_tool_version()
    resp = jsonify(tool_version=tool_version)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/tool_version/<tool_version_id>/task_templates', methods=['GET'])
def get_task_templates(tool_version_id: int):
    """Get the Tool Version."""
    # check input variable
    app.logger = app.logger.bind(tool_version_id=tool_version_id)
    app.logger.info("Getting available task_templates")

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
