"""Routes for TaskTemplate."""
from http import HTTPStatus as StatusCodes
import json
import pandas as pd
from typing import Any, cast, Dict, List, Set

from flask import jsonify, request
from flask_cors import cross_origin
from sqlalchemy import func, select, update
import structlog

from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus
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
@cross_origin()
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


@blueprint.route("/workflow/<workflow_id>/workflow_tasks", methods=["GET"])
def get_workflow_tasks(workflow_id: int) -> Any:
    """Get the tasks for a given workflow."""
    limit = request.args.get("limit")
    status_request = request.args.getlist("status", None)
    logger.debug(f"Get tasks for workflow in status {status_request}")

    session = SessionLocal()
    with session.begin():
        if status_request:
            query_filter = [Workflow.id == Task.workflow_id,
                            Task.status.in_([i for arg in status_request for i in _reversed_cli_label_mapping[arg]]),
                            Workflow.id == int(workflow_id)]
        else:
            query_filter = [Workflow.id == Task.workflow_id,
                            Workflow.id == int(workflow_id)]
        sql = (
            select(Task.id,
                   Task.name,
                   Task.status,
                   Task.num_attempts
                   ).where(*query_filter)
        ).order_by(Task.id.desc())
        rows = session.execute(sql).all()
    column_names = ("TASK_ID", "TASK_NAME", "STATUS", "RETRIES")
    res = [dict(zip(column_names, ti)) for ti in rows]
    for r in res:
        r["RETRIES"] = 0 if r["RETRIES"] <= 1 else r["RETRIES"] - 1

    if limit:
        res = res[:int(limit)]

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
    logger.debug(f"Validate user name {username} for workflow")
    session = SessionLocal()
    with session.begin():
        query_filter = [WorkflowRun.workflow_id == workflow_id]
        sql = (
            select(WorkflowRun.user).where(*query_filter)
        ).distinct()
        rows = session.execute(sql).all()
    usernames = [row[0] for row in rows]

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
    session = SessionLocal()
    with session.begin():
        query_filter = [WorkflowRun.workflow_id == workflow_id,
                        WorkflowRun.status == "E"]
        sql = (
            select(WorkflowRun.id,
                   WorkflowRun.user
                   ).where(*query_filter)
        ).order_by(WorkflowRun.created_date.desc())
        rows = session.execute(sql).all()
    result = None if len(rows) <=0 else rows[0]
    if result is not None and result[1] == username:
        resp = jsonify({"workflow_run_id": result[0]})
    else:
        resp = jsonify({"workflow_run_id": None})

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("workflow/<workflow_id>/reset", methods=["PUT"])
def reset_workflow(workflow_id: int) -> Any:
    """Update the workflow's status, all its tasks' statuses to 'G'."""
    session = SessionLocal()
    with session.begin():
        update_stmt = update(
            Workflow
        ).where(
            Workflow.id == workflow_id
        ).values(status="G", status_date=func.now())
        session.execute(update_stmt)
        update_stmt = update(
            Task
        ).where(
            Task.workflow_id == workflow_id
        ).values(status="G", status_date=func.now(), num_attempts = 0)
        session.execute(update_stmt)
        session.commit()

    resp = jsonify({})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_status", methods=["GET"])
@cross_origin()
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
    # convert workflow request into sql filter
    if workflow_request:
        workflow_request = [int(w) for w in workflow_request]
        params["workflow_id"] = workflow_request
        where_clause = "WHERE workflow.id in :workflow_id "
    else:  # if we don't specify workflow then we use the users
        # convert user request into sql filter
        # directly producing workflow_ids, and thus where_clause
        if user_request:
            session = SessionLocal()
            with session.begin():
                query_filter = [WorkflowRun.user.in_(user_request)]
                sql = (
                    select(WorkflowRun.workflow_id).where(*query_filter)
                ).distinct()
                rows = session.execute(sql).all()
            workflow_request = [int(row[0]) for row in rows]

    # execute query
    session = SessionLocal()
    with session.begin():
        query_filter = [Workflow.id == Task.workflow_id,
                        WorkflowStatus.id == Workflow.status,
                        Workflow.id.in_(workflow_request)]
        sql = (
            select(
                Workflow.id,
                Workflow.name,
                WorkflowStatus.label,
                func.count(Task.status),
                Task.status,
                Workflow.created_date,
            ).where(*query_filter)
        ).group_by(Workflow.id, Task.status, Workflow.name, WorkflowStatus.label)
        rows = session.execute(sql).all()
    column_names = ("WF_ID", "WF_NAME", "WF_STATUS", "TASKS", "STATUS", "CREATED_DATE")
    rows = [dict(zip(column_names, ti)) for ti in rows]
    res = []
    for r in rows:
        session = SessionLocal()
        with session.begin():
            q_filter = [Task.workflow_id == r["WF_ID"],
                        Task.status == r["STATUS"]]
            q = (
                select(Task.num_attempts).where(*q_filter)
            )
            query_result = session.execute(q).all()
        retries = 0
        for rr in query_result:
            retries += 0 if int(rr[0]) <= 1 else int(rr[0]) - 1
        r["RETRIES"] = retries
        res.append(r)

    if res is not None and len(res) > 0:
        if limit:
            res = res[:int(limit)]

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


@blueprint.route("/workflow_status_viz/<username>", methods=["GET"])
@cross_origin()
def workflow_status_by_user(username: str) -> Any:
    """Fetch associated workflows and workflow runs by user name."""

    session = SessionLocal()
    with session.begin():
        sql = (
            select(
                Workflow.id,
                Tool.name,
                Workflow.name,
                Workflow.created_date,
                WorkflowStatus.label,
                WorkflowRun.id,
                WorkflowRunStatus.label,
                Task.status,
                func.count(Task.status)
            ).where(
                WorkflowRun.user == username,
                WorkflowRun.workflow_id == Workflow.id,
                Workflow.tool_version_id == ToolVersion.id,
                ToolVersion.tool_id == Tool.id,
                Task.workflow_id == Workflow.id,
                Workflow.status == WorkflowStatus.id,
                WorkflowRun.status == WorkflowRunStatus.id,
            ).group_by(
                Workflow.id, Tool.name, Workflow.name, Workflow.created_date,
                Workflow.status, WorkflowRun.id, WorkflowRun.status, Task.status
            ).order_by(
                Workflow.created_date.desc()
            )
        )
        rows = session.execute(sql).all()

    column_names = ("wf_id", "wf_tool", "wf_name", "wf_submitted_date",
                    "wf_status", "wfr_id", "wfr_status", "task_status", "task_count")

    # Convert to a dataframe in order to pivot wide
    df = pd.DataFrame([wf for wf in rows], columns=column_names)

    # Group the statuses into pending, running, done, and fatal
    df.replace({"task_status": _cli_label_mapping}, inplace=True)

    # Pivot wide and limit to 30 records to avoid returning thousands of workflows
    # TODO: make the limit configurable
    df_wide = pd.pivot_table(
        df, index=column_names[:-2], columns='task_status', values='task_count', fill_value=0
    )
    df_wide.sort_values(by=['wf_id'], ascending=False, inplace=True)
    df_wide = df_wide.reset_index()[:30]
    # Initialize the missing statuses if needed
    for status_type in _reversed_cli_label_mapping:
        if status_type not in df_wide:
            df_wide[status_type] = 0

    return_dict = df_wide.to_dict(orient='records')

    res = jsonify(workflows=return_dict)
    res.return_code = StatusCodes.OK
    return res
