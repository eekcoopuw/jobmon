"""Routes for Tasks."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import current_app as app, jsonify, request
from jobmon.server.web.models import DB
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.arg_type import ArgType
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.server_side_exception import InvalidUsage
import sqlalchemy
from sqlalchemy.sql import text

from jobmon.server.web.routes import finite_state_machine


@finite_state_machine.route('/task_template', methods=['POST'])
def get_task_template() -> Any:
    """Add a task template for a given tool to the database."""
    # check input variable
    data = request.get_json()
    try:
        tool_version_id = int(data["tool_version_id"])
        app.logger = app.logger.bind(tool_version_id=tool_version_id)
        app.logger.info(f"Add task tamplate for tool_version_id {tool_version_id} ")

    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    # add to DB
    try:
        tt = TaskTemplate(tool_version_id=data["tool_version_id"],
                          name=data["task_template_name"])
        DB.session.add(tt)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM task_template
            WHERE
                tool_version_id = :tool_version_id
                AND name = :name
        """
        tt = DB.session.query(TaskTemplate).from_statement(text(query)).params(
            tool_version_id=data["tool_version_id"],
            name=data["task_template_name"]
        ).one()
        DB.session.commit()
    resp = jsonify(task_template_id=tt.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route('/task_template/<task_template_id>/versions', methods=['GET'])
def get_task_template_versions(task_template_id: int) -> Any:
    """Get the task_template_version."""
    # get task template version object
    app.logger = app.logger.bind(task_template_id=task_template_id)
    app.logger.info(f"Getting task template version for task template: {task_template_id}")

    query = """
        SELECT
            task_template_version.*
        FROM task_template_version
        WHERE
            task_template_id = :task_template_id
    """
    ttvs = DB.session.query(TaskTemplateVersion).from_statement(text(query)).params(
        task_template_id=task_template_id
    ).all()

    wire_obj = [ttv.to_wire_as_client_task_template_version() for ttv in ttvs]

    resp = jsonify(task_template_versions=wire_obj)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_arg(name: str) -> Arg:
    try:
        arg = Arg(name=name)
        DB.session.add(arg)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM arg
            WHERE name = :name
        """
        arg = DB.session.query(Arg).from_statement(text(query)).params(name=name).one()
        DB.session.commit()
    return arg


@finite_state_machine.route('/task_template/<task_template_id>/add_version', methods=['POST'])
def add_task_template_version(task_template_id: int) -> Any:
    """Add a tool to the database."""
    # check input variables
    app.logger = app.logger.bind(task_template_id=task_template_id)
    data = request.get_json()
    app.logger.info(f"Add tool for task_template_id {task_template_id}")
    if task_template_id is None:
        raise InvalidUsage(f"Missing variable task_template_id in {request.path}",
                           status_code=400)
    try:
        int(task_template_id)
        # populate the argument table
        arg_mapping_dct: dict = {ArgType.NODE_ARG: [],
                                 ArgType.TASK_ARG: [],
                                 ArgType.OP_ARG: []}
        for arg_name in data["node_args"]:
            arg_mapping_dct[ArgType.NODE_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["task_args"]:
            arg_mapping_dct[ArgType.TASK_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["op_args"]:
            arg_mapping_dct[ArgType.OP_ARG].append(_add_or_get_arg(arg_name))
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    try:
        ttv = TaskTemplateVersion(task_template_id=task_template_id,
                                  command_template=data["command_template"],
                                  arg_mapping_hash=data["arg_mapping_hash"])
        DB.session.add(ttv)
        DB.session.commit()

        # get a lock
        DB.session.refresh(ttv, with_for_update=True)

        for arg_type_id in arg_mapping_dct.keys():
            for arg in arg_mapping_dct[arg_type_id]:
                ctatm = TemplateArgMap(
                    task_template_version_id=ttv.id,
                    arg_id=arg.id,
                    arg_type_id=arg_type_id)
                DB.session.add(ctatm)
        DB.session.commit()
        resp = jsonify(task_template_version=ttv.to_wire_as_client_task_template_version())
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        # if another process is adding this task_template_version then this query should block
        # until the template_arg_map has been populated and committed
        query = """
            SELECT *
            FROM task_template_version
            WHERE
                task_template_id = :task_template_id
                AND command_template = :command_template
                AND arg_mapping_hash = :arg_mapping_hash
        """
        ttv = DB.session.query(TaskTemplateVersion).from_statement(text(query)).params(
            task_template_id=task_template_id,
            command_template=data["command_template"],
            arg_mapping_hash=data["arg_mapping_hash"]
        ).one()
        DB.session.commit()
        resp = jsonify(
            task_template_version=ttv.to_wire_as_client_task_template_version()
        )
        resp.status_code = StatusCodes.OK
        return resp
