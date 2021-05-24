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
    # input variable check
    data = request.get_json()
    try:
        name = data["name"]
        app.logger = app.logger.bind(tool_name=name)
    except KeyError as e:
        raise InvalidUsage(f"Parameter name is missing in path {request.path}",
                           status_code=400) from e

    # add tool to db
    try:
        app.logger.info(f"Adding tool {name}")
        tool = Tool(name=name)
        DB.session.add(tool)
        DB.session.commit()
        tool = tool.to_wire_as_client_tool()
        resp = jsonify(tool=tool)
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        tool = None
        resp = jsonify(tool=tool)
        resp.status_code = StatusCodes.OK
        return resp


@jobmon_client.route('/tool/<tool_name>', methods=['GET'])
def get_tool(tool_name: str):
    """Get the Tool object from the database."""
    # get data from db
    app.logger = app.logger.bind(tool_name=tool_name)
    app.logger.info(f"Getting tool by name: {tool_name}")
    query = """
        SELECT
            tool.*
        FROM
            tool
        WHERE
            name = :tool_name"""
    tool = DB.session.query(Tool).from_statement(text(query)).params(
        tool_name=tool_name
    ).one_or_none()
    DB.session.commit()
    if tool:
        try:
            tool = tool.to_wire_as_client_tool()
            resp = jsonify(tool=tool)
            resp.status_code = StatusCodes.OK
            return resp
        except Exception as e:
            raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                              status_code=500) from e
    else:
        raise InvalidUsage(f"Tool {tool_name} does not exist with request to {request.path}",
                           status_code=400)


@jobmon_client.route('/tool/<tool_id>/tool_versions', methods=['GET'])
def get_tool_versions(tool_id: int):
    """Get the Tool Version."""
    # check input variable
    app.logger = app.logger.bind(tool_id=tool_id)
    app.logger.info(f"Getting available tool versions for tool_id {tool_id}")
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
    app.logger.info(f"Tool version for {tool_id} is {tool_versions}")
    resp = jsonify(tool_versions=tool_versions)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/tool_version', methods=['POST'])
def add_tool_version():
    """Add a new version for a Tool."""
    # check input variable
    data = request.get_json()
    try:
        tool_id = int(data["tool_id"])
        app.logger = app.logger.bind(tool_id=tool_id)
        app.logger.info(f"Creating tool_version for tool_id {tool_id}")
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    tool_version = ToolVersion(tool_id=tool_id)
    DB.session.add(tool_version)
    DB.session.commit()
    tool_version = tool_version.to_wire_as_client_tool_version()
    resp = jsonify(tool_version=tool_version)
    resp.status_code = StatusCodes.OK
    return resp
