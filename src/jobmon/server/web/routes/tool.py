"""Routes for Tools"""
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.server_side_exception import (InvalidUsage, ServerError)

import sqlalchemy
from sqlalchemy.sql import text

from . import jobmon_client


@jobmon_client.route('/tool', methods=['POST'])
def add_tool():
    """Add a tool to the database"""
    data = request.get_json()
    tool_name = data["name"]
    # add tool to db
    try:
        tool = Tool(name=tool_name)
        DB.session.add(tool)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT
                tool.*
            FROM
                tool
            WHERE
                name = :tool_name"""
        tool = DB.session.query(Tool).from_statement(text(query)).params(
            tool_name=tool_name
        ).one()

    wire_format = tool.to_wire_as_client_tool()
    resp = jsonify(tool=wire_format)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/tool/<tool_id>/tool_versions', methods=['GET'])
def get_tool_versions(tool_id: int):
    """Get the Tool Version."""
    # check input variable
    app.logger = app.logger.bind(tool_id=tool_id)
    app.logger.info("Getting available tool versions")
    if tool_id is None:
        raise InvalidUsage(f"Variable tool_id is None in {request.path}", status_code=400)
    try:
        int(tool_id)
    except Exception as e:
        raise InvalidUsage(f"Variable tool_id must be an int in {request.path}",
                           status_code=400) from e

    # get data from db
    query = """
        SELECT
            tool_version.*
        FROM
            tool_version
        WHERE
            tool_id = :tool_id"""
    tool_versions = DB.session.query(ToolVersion).from_statement(text(query)).params(
        tool_id=tool_id
    ).all()
    DB.session.commit()
    tool_versions = [t.to_wire_as_client_tool_version() for t in tool_versions]
    resp = jsonify(tool_versions=tool_versions)
    resp.status_code = StatusCodes.OK
    return resp
