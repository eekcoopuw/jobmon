import json
import os
from http import HTTPStatus as StatusCodes
from typing import Dict, List, Set, Union


from flask import Blueprint, current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.arg_type import ArgType
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.server_side_exception import (InvalidUsage, ServerError)

import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import func, text

from . import jobmon_client, jobmon_cli



def _add_workflow_attributes(workflow_id: int, workflow_attributes: Dict[str, str]):
    # add attribute
    wf_attributes_list = []
    for name, val in workflow_attributes.items():
        wf_type_id = _add_or_get_wf_attribute_type(name)
        wf_attribute = WorkflowAttribute(workflow_id=workflow_id,
                                         workflow_attribute_type_id=wf_type_id,
                                         value=val)
        wf_attributes_list.append(wf_attribute)
    DB.session.add_all(wf_attributes_list)
    DB.session.flush()


@jobmon_client.route('/workflow', methods=['POST'])
def bind_workflow():
    """Bind a workflow to the database."""
    data = request.get_json()
    try:
        data = request.get_json()
        tv_id = int(data['tool_version_id'])
        dag_id = int(data['dag_id'])
        whash = int(data['workflow_args_hash'])
        thash = int(data['task_hash'])
        description = data['description']
        name = data["name"]
        workflow_args = data["workflow_args"]
        max_concurrently_running = data['max_concurrently_running']
        workflow_attributes = data["workflow_attributes"]
        app.logger = app.logger.bind(dag_id=dag_id, tool_version_id=tv_id,
                                     workflow_args_hash=whash, task_hash=thash)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    query = """
        SELECT workflow.*
        FROM workflow
        WHERE
            tool_version_id = :tool_version_id
            AND dag_id = :dag_id
            AND workflow_args_hash = :workflow_args_hash
            AND task_hash = :task_hash
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        tool_version_id=tv_id,
        dag_id=dag_id,
        workflow_args_hash=whash,
        task_hash=thash
    ).one_or_none()
    if workflow is None:
        # create a new workflow
        workflow = Workflow(tool_version_id=tv_id,
                            dag_id=dag_id,
                            workflow_args_hash=whash,
                            task_hash=thash,
                            description=description,
                            name=name,
                            workflow_args=workflow_args,
                            max_concurrently_running=max_concurrently_running)
        DB.session.add(workflow)
        DB.session.commit()

        # update attributes
        if workflow_attributes:
            _add_workflow_attributes(workflow.id, workflow_attributes)
            DB.session.commit()

        newly_created = True
    else:
        newly_created = False

    resp = jsonify({'workflow_id': workflow.id, 'status': workflow.status,
                    'newly_created': newly_created})
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_args_hash>', methods=['GET'])
def get_matching_workflows_by_workflow_args(workflow_args_hash: int):
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        int(workflow_args_hash)
        app.logger = app.logger.bind(workflow_args_hash=workflow_args_hash)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    query = """
        SELECT workflow.task_hash, workflow.tool_version_id, dag.hash
        FROM workflow
        JOIN dag
            ON workflow.dag_id = dag.id
        WHERE
            workflow.workflow_args_hash = :workflow_args_hash
    """

    res = DB.session.query(Workflow.task_hash, Workflow.tool_version_id,
                           Dag.hash).from_statement(text(query)).params(
        workflow_args_hash=workflow_args_hash
    ).all()
    DB.session.commit()
    res = [(row.task_hash, row.tool_version_id, row.hash) for row in res]
    resp = jsonify(matching_workflows=res)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_wf_attribute_type(name: str) -> int:
    try:
        wf_attrib_type = WorkflowAttributeType(name=name)
        DB.session.add(wf_attrib_type)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
        SELECT id, name
        FROM workflow_attribute_type
        WHERE name = :name
        """
        wf_attrib_type = DB.session.query(WorkflowAttributeType)\
            .from_statement(text(query)).params(name=name).one()

    return wf_attrib_type.id


def _upsert_wf_attribute(workflow_id: int, name: str, value: str):
    wf_attrib_id = _add_or_get_wf_attribute_type(name)
    insert_vals = insert(WorkflowAttribute).values(
        workflow_id=workflow_id,
        workflow_attribute_type_id=wf_attrib_id,
        value=value
    )

    upsert_stmt = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value,
        status='U')

    DB.session.execute(upsert_stmt)
    DB.session.commit()


@jobmon_client.route('/workflow/<workflow_id>/workflow_attributes', methods=['PUT'])
def update_workflow_attribute(workflow_id: int):
    """Update the attributes for a given workflow."""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    try:
        int(workflow_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    """ Add/update attributes for a workflow """
    data = request.get_json()
    attributes = data["workflow_attributes"]
    if attributes:
        for name, val in attributes.items():
            _upsert_wf_attribute(workflow_id, name, val)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_id>/set_resume', methods=['POST'])
def set_resume(workflow_id: int):
    """Set resume on a workflow"""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    try:
        data = request.get_json()
        reset_running_jobs = bool(data['reset_running_jobs'])
        description = str(data['description'])
        name = str(data["name"])
        max_concurrently_running = int(data['max_concurrently_running'])
        workflow_attributes = data["workflow_attributes"]
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        workflow_id=workflow_id
    ).one()

    # set mutible attribute
    workflow.description = description
    workflow.name = name
    workflow.max_concurrently_running = max_concurrently_running
    DB.session.commit()

    # trigger resume on active workflow run
    workflow.resume(reset_running_jobs)
    DB.session.commit()

    # update attributes
    if workflow_attributes:
        _add_workflow_attributes(workflow.id, workflow_attributes)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_id>/is_resumable', methods=['GET'])
def workflow_is_resumable(workflow_id: int):
    """Check if a workflow is in a resumable state"""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        workflow_id=workflow_id
    ).one()
    DB.session.commit()

    resp = jsonify(workflow_is_resumable=workflow.is_resumable)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('workflow/<workflow_id>/update_max_running', methods=['PUT'])
def update_max_running(workflow_id):
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = request.get_json()
    try:
        new_limit = data['max_tasks']
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    q = """
        UPDATE workflow
        SET max_concurrently_running = {new_limit}
        WHERE id = {workflow_id}
    """.format(new_limit=new_limit, workflow_id=workflow_id)

    res = DB.session.execute(q)
    DB.session.commit()

    if res.rowcount == 0:  # Return a warning message if no update was performed
        message = f"No update performed for workflow ID {workflow_id}, max_concurrency is " \
                  f"{new_limit}"
    else:
        message = f"Workflow ID {workflow_id} max concurrency updated to {new_limit}"

    resp = jsonify(message=message)
    resp.status_code = StatusCodes.OK
    return resp
