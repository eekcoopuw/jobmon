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



@jobmon_client.route('/workflow_run', methods=['POST'])
def add_workflow_run():
    """Add a workflow run to the db."""
    try:
        data = request.get_json()
        wid = data["workflow_id"]
        int(wid)
        app.logger = app.logger.bind(workflow_id=wid)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    workflow_run = WorkflowRun(
        workflow_id=wid,
        user=data["user"],
        executor_class=data["executor_class"],
        jobmon_version=data["jobmon_version"],
        status=WorkflowRunStatus.REGISTERED
    )
    DB.session.add(workflow_run)
    DB.session.commit()
    resp = jsonify(workflow_run_id=workflow_run.id)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run/<workflow_run_id>/link', methods=['POST'])
def link_workflow_run(workflow_run_id: int):
    """Link this workflow run to a workflow."""
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
    """
    workflow_run = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_id=workflow_run_id
    ).one()

    # refresh with lock in case other workflow run is trying to progress
    workflow = workflow_run.workflow
    DB.session.refresh(workflow, with_for_update=True)

    # check if any workflow run is in linked state.
    # if not any linked, proceed.
    current_wfr = workflow.link_workflow_run(workflow_run)
    DB.session.commit()  # release lock
    resp = jsonify(current_wfr=current_wfr)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
def terminate_workflow_run(workflow_run_id: int):
    """Terminate a workflow run and get its tasks in order."""
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    try:
        int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()

    if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
        states = [TaskStatus.INSTANTIATED]
    elif workflow_run.status == WorkflowRunStatus.COLD_RESUME:
        states = [TaskStatus.INSTANTIATED, TaskInstanceStatus.RUNNING]

    # add error logs
    log_errors = """
        INSERT INTO task_instance_error_log
            (task_instance_id, description, error_time)
        SELECT
            task_instance.id,
            CONCAT(
                'Workflow resume requested. Setting to K from status of: ',
                task_instance.status
            ) as description,
            CURRENT_TIMESTAMP as error_time
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task.status IN :states
    """
    DB.session.execute(log_errors,
                       {"workflow_run_id": int(workflow_run_id),
                        "states": states})
    DB.session.flush()

    # update job instance states
    update_task_instance = """
        UPDATE
            task_instance
        JOIN task
            ON task_instance.task_id = task.id
        SET
            task_instance.status = 'K',
            task_instance.status_date = CURRENT_TIMESTAMP
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task.status IN :states
    """
    DB.session.execute(update_task_instance,
                       {"workflow_run_id": workflow_run_id,
                        "states": states})
    DB.session.flush()

    # transition to terminated
    workflow_run.transition(WorkflowRunStatus.TERMINATED)
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run_status', methods=['GET'])
def get_active_workflow_runs():
    """Return all workflow runs that are currently in the specified state."""
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.status in :workflow_run_status
    """
    workflow_runs = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_status=request.args.getlist('status')
    ).all()
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def log_workflow_run_heartbeat(workflow_run_id: int):
    """Log a heartbeat on behalf of the workflow run to show that the client side is still
    alive.
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    data = request.get_json()
    app.logger.debug(f"Heartbeat data: {data}")

    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()

    try:
        workflow_run.status
        workflow_run.heartbeat(data["next_report_increment"], WorkflowRunStatus.LINKING)
        DB.session.commit()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition:
        DB.session.rollback()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat rolled back")

    resp = jsonify(message=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp
