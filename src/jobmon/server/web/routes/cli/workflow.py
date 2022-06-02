"""Routes for TaskTemplate."""
from http import HTTPStatus as StatusCodes
import json
import numpy as np
from typing import Any, cast, Dict, List, Set

from flask import jsonify, request
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
import structlog

from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.cli import blueprint

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)

_cli_label_mapping = {
    "A": "PENDING",
    "G": "PENDING",
    "Q": "PENDING",
    "I": "PENDING",
    "E": "PENDING",
    "R": "RUNNING",
    "F": "FATAL",
    "D": "DONE",
}

_reversed_cli_label_mapping = {
    "PENDING": ["A", "G", "Q", "I", "E"],
    "RUNNING": ["R"],
    "FATAL": ["F"],
    "DONE": ["D"],
}

_cli_order = ["PENDING", "RUNNING", "DONE", "FATAL"]

@blueprint.route("/workflow_validation", methods=["POST"])
def get_workflow_validation_status() -> Any:
    """Check if workflow is valid."""
    # initial params
    data = request.get_json()
    task_ids = data["task_ids"]

    # if the given list is empty, return True
    if len(task_ids) == 0:
        resp = jsonify(validation=True)
        resp.status_code = StatusCodes.OK
        return resp

    session = SessionLocal()
    with session.begin():
        # execute query
        query_filter = [Task.workflow_id == Workflow.id,
                        Task.id.in_(task_ids)]
        sql = (
            select(
                Task.workflow_id,
                Workflow.status
            ).where(*query_filter)
        ).distinct()
        rows = session.execute(sql).all()
    res = [ti[1] for ti in rows]
    # Validate if all tasks are in the same workflow and the workflow status is dead
    if len(res) == 1 and res[0] in (
        Statuses.FAILED,
        Statuses.DONE,
        Statuses.ABORTED,
        Statuses.HALTED,
    ):
        validation = True
    else:
        validation = False

    resp = jsonify(validation=validation, workflow_status=res[0])
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_status", methods=["GET"])
def get_workflow_status() -> Any:
    """Get the status of the workflow."""
    # initial params
    params = {}
    user_request = request.args.getlist("user")
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    workflow_request = request.args.getlist("workflow_id")
    logger.debug(f"Query for wf {workflow_request} status.")
    if workflow_request == "all":  # specifying all is equivalent to None
        workflow_request = []
    limit = request.args.get("limit")
    where_clause = ""
    # convert workflow request into sql filter
    if workflow_request:
        workflow_request = [int(w) for w in workflow_request]
        params["workflow_id"] = workflow_request
        where_clause = "WHERE workflow.id in :workflow_id "
    else:  # if we don't specify workflow then we use the users
        # convert user request into sql filter
        # directly producing workflow_ids, and thus where_clause
        if user_request:
            params["user"] = user_request
            where_clause_user = "WHERE workflow_run.user in :user "
            q_user = """
                SELECT DISTINCT workflow_id
                FROM workflow_run
                {where_clause}
            """.format(
                where_clause=where_clause_user
            )
            res_user = DB.session.execute(q_user, params).fetchall()
            workflow_ids = [int(row.workflow_id) for row in res_user]
            params["workflow_id"] = workflow_ids
            where_clause = "WHERE workflow.id in :workflow_id "
    # execute query
    q = """
        SELECT
            workflow.id as WF_ID,
            workflow.name as WF_NAME,
            workflow_status.label as WF_STATUS,
            count(task.status) as TASKS,
            task.status AS STATUS,
            workflow.created_date as CREATED_DATE,
            sum(
                CASE
                    WHEN num_attempts <= 1 THEN 0
                    ELSE num_attempts - 1
                END
            ) as RETRIES
        FROM workflow
        JOIN task
            ON workflow.id = task.workflow_id
        JOIN workflow_status
            ON workflow_status.id = workflow.status
        {where_clause}
        GROUP BY workflow.id, task.status, workflow.name, workflow_status.label
        ORDER BY workflow.id desc
    """.format(
        where_clause=where_clause
    )
    if limit:
        q = f"{q}\nLIMIT {limit}"
    res = DB.session.execute(q, params).fetchall()

    if res:

        # assign to dataframe for aggregation
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)

        # aggregate totals by workflow and status
        df = df.groupby(
            ["WF_ID", "WF_NAME", "WF_STATUS", "STATUS", "CREATED_DATE"]
        ).agg({"TASKS": "sum", "RETRIES": "sum"})

        # pivot wide by task status
        tasks = df.pivot_table(
            values="TASKS",
            index=["WF_ID", "WF_NAME", "WF_STATUS", "CREATED_DATE"],
            columns="STATUS",
            fill_value=0,
        )
        for col in _cli_order:
            if col not in tasks.columns:
                tasks[col] = 0
        tasks = tasks[_cli_order]

        # aggregate again without status to get the totals by workflow
        retries = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS", "CREATED_DATE"]).agg(
            {"TASKS": "sum", "RETRIES": "sum"}
        )

        # combine datasets
        df = pd.concat([tasks, retries], axis=1)

        # compute pcts and format
        for col in _cli_order:
            df[col + "_pct"] = (df[col].astype(float) / df["TASKS"].astype(float)) * 100
            df[col + "_pct"] = df[[col + "_pct"]].round(1)
            df[col] = (
                df[col].astype(int).astype(str)
                + " ("
                + df[col + "_pct"].astype(str)
                + "%)"
            )

        # df.replace(to_replace={"0 (0.0%)": "NA"}, inplace=True)
        # final order
        df = df[["TASKS"] + _cli_order + ["RETRIES"]]
        df = df.reset_index()
        df = df.to_json()
        resp = jsonify(workflows=df)
    else:
        df = pd.DataFrame(
            {},
            columns=[
                "WF_ID",
                "WF_NAME",
                "WF_STATUS",
                "CREATED_DATE",
                "TASKS",
                "PENDING",
                "RUNNING",
                "DONE",
                "FATAL",
                "RETRIES",
            ],
        ).to_json()
        resp = jsonify(workflows=df)

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/workflow_tasks", methods=["GET"])
def get_workflow_tasks(workflow_id: int) -> Any:
    """Get the tasks for a given workflow."""
    params: Dict = {"workflow_id": workflow_id}
    bind_to_logger(workflow_id=workflow_id)
    limit = request.args.get("limit")
    where_clause = "WHERE workflow.id = :workflow_id"
    status_request = request.args.getlist("status", None)
    logger.debug(f"Get tasks for workflow in status {status_request}")

    if status_request:
        params["status"] = [
            i for arg in status_request for i in _reversed_cli_label_mapping[arg]
        ]
        where_clause += " AND task.status in :status"
    q = """
        SELECT
            task.id AS TASK_ID,
            task.name AS TASK_NAME,
            task.status AS STATUS,
            CASE
                WHEN num_attempts <= 1 THEN 0
                ELSE num_attempts - 1
            END AS RETRIES
        FROM workflow
        JOIN task
            ON workflow.id = task.workflow_id
        {where_clause}""".format(
        where_clause=where_clause
    )
    if limit:
        q = f"{q}\nLIMIT {limit}"
    res = DB.session.execute(q, params).fetchall()
    logger.debug(
        f"The following tasks of workflow are in status {status_request}:\n{res}"
    )
    if res:
        # assign to dataframe for serialization
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)
        df = df.to_json()
        resp = jsonify(workflow_tasks=df)
    else:
        df = pd.DataFrame({}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
        resp = jsonify(workflow_tasks=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/validate_username/<username>", methods=["GET"]
)
def get_workflow_user_validation(workflow_id: int, username: str) -> Any:
    """Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    bind_to_logger(workflow_id=workflow_id)
    logger.debug(f"Validate user name {username} for workflow")
    query = """
        SELECT DISTINCT user
        FROM workflow_run
        WHERE workflow_run.workflow_id = {workflow_id}
    """.format(
        workflow_id=workflow_id
    )

    result = DB.session.execute(query)

    usernames = [row.user for row in result]

    resp = jsonify(validation=username in usernames)

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/validate_for_workflow_reset/<username>", methods=["GET"]
)
def get_workflow_run_for_workflow_reset(workflow_id: int, username: str) -> Any:
    """Last workflow_run_id associated with a given workflow_id started by the username.

    Used to validate for workflow_reset:
        1. The last workflow_run of the current workflow must be in error state.
        2. This last workflow_run must have been started by the input username.
        3. This last workflow_run is in status 'E'
    """
    query = """
        SELECT id AS workflow_run_id, user AS username
        FROM workflow_run
        WHERE workflow_run.workflow_id = {workflow_id} and workflow_run.status = 'E'
        ORDER BY created_date DESC
        LIMIT 1
    """.format(
        workflow_id=workflow_id
    )

    result = DB.session.execute(query).one_or_none()
    if result is not None and result.username == username:
        resp = jsonify({"workflow_run_id": result.workflow_run_id})
    else:
        resp = jsonify({"workflow_run_id": None})

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("workflow/<workflow_id>/reset", methods=["PUT"])
def reset_workflow(workflow_id: int) -> Any:
    """Update the workflow's status, all its tasks' statuses to 'G'."""
    q_workflow = """
        UPDATE workflow
        SET status = 'G', status_date = CURRENT_TIMESTAMP
        WHERE id = {workflow_id}
    """.format(
        workflow_id=workflow_id
    )

    DB.session.execute(q_workflow)

    q_task = """
        UPDATE task
        SET status = 'G', status_date = CURRENT_TIMESTAMP, num_attempts = 0
        WHERE workflow_id = {workflow_id}
    """.format(
        workflow_id=workflow_id
    )

    DB.session.execute(q_task)

    DB.session.commit()

    resp = jsonify({})
    resp.status_code = StatusCodes.OK
    return resp
