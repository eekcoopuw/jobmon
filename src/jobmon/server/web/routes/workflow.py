"""Routes for Workflows."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any, Dict

from flask import jsonify, request
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import text
from werkzeug.local import LocalProxy

from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.server_side_exception import InvalidUsage


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))

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


def _add_workflow_attributes(
    workflow_id: int, workflow_attributes: Dict[str, str]
) -> None:
    # add attribute
    bind_to_logger(workflow_id=workflow_id)
    logger.info(f"Add Attributes: {workflow_attributes}")
    wf_attributes_list = []
    for name, val in workflow_attributes.items():
        wf_type_id = _add_or_get_wf_attribute_type(name)
        wf_attribute = WorkflowAttribute(
            workflow_id=workflow_id, workflow_attribute_type_id=wf_type_id, value=val
        )
        wf_attributes_list.append(wf_attribute)
        logger.debug(f"Attribute name: {name}, value: {val}")
    DB.session.add_all(wf_attributes_list)
    DB.session.flush()


@finite_state_machine.route("/workflow", methods=["POST"])
def bind_workflow() -> Any:
    """Bind a workflow to the database."""
    try:
        data = request.get_json()
        tv_id = int(data["tool_version_id"])
        dag_id = int(data["dag_id"])
        whash = int(data["workflow_args_hash"])
        thash = int(data["task_hash"])
        description = data["description"]
        name = data["name"]
        workflow_args = data["workflow_args"]
        max_concurrently_running = data["max_concurrently_running"]
        workflow_attributes = data["workflow_attributes"]
        bind_to_logger(
            dag_id=dag_id,
            tool_version_id=tv_id,
            workflow_args_hash=str(whash),
            task_hash=str(thash),
        )
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    logger.info("Bind workflow")
    query = """
        SELECT workflow.*
        FROM workflow
        WHERE
            tool_version_id = :tool_version_id
            AND dag_id = :dag_id
            AND workflow_args_hash = :workflow_args_hash
            AND task_hash = :task_hash
    """
    workflow = (
        DB.session.query(Workflow)
        .from_statement(text(query))
        .params(
            tool_version_id=tv_id,
            dag_id=dag_id,
            workflow_args_hash=whash,
            task_hash=thash,
        )
        .one_or_none()
    )
    if workflow is None:
        # create a new workflow
        workflow = Workflow(
            tool_version_id=tv_id,
            dag_id=dag_id,
            workflow_args_hash=whash,
            task_hash=thash,
            description=description,
            name=name,
            workflow_args=workflow_args,
            max_concurrently_running=max_concurrently_running,
        )
        DB.session.add(workflow)
        DB.session.commit()
        logger.info("Created new workflow")

        # update attributes
        if workflow_attributes:
            _add_workflow_attributes(workflow.id, workflow_attributes)
            DB.session.commit()
        newly_created = True
    else:
        newly_created = False

    resp = jsonify(
        {
            "workflow_id": workflow.id,
            "status": workflow.status,
            "newly_created": newly_created,
        }
    )
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow/<workflow_args_hash>", methods=["GET"])
def get_matching_workflows_by_workflow_args(workflow_args_hash: int) -> Any:
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        int(workflow_args_hash)
        bind_to_logger(workflow_args_hash=str(workflow_args_hash))
        logger.info(f"Looking for wf with hash {workflow_args_hash}")
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    query = """
        SELECT workflow.task_hash, workflow.tool_version_id, dag.hash
        FROM workflow
        JOIN dag
            ON workflow.dag_id = dag.id
        WHERE
            workflow.workflow_args_hash = :workflow_args_hash
    """

    res = (
        DB.session.query(Workflow.task_hash, Workflow.tool_version_id, Dag.hash)
        .from_statement(text(query))
        .params(workflow_args_hash=workflow_args_hash)
        .all()
    )
    DB.session.commit()
    if len(res) > 0:
        logger.debug(
            f"Found {res} workflow for " f"workflow_args_hash {workflow_args_hash}"
        )
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
        wf_attrib_type = (
            DB.session.query(WorkflowAttributeType)
            .from_statement(text(query))
            .params(name=name)
            .one()
        )

    return wf_attrib_type.id


def _upsert_wf_attribute(workflow_id: int, name: str, value: str) -> None:
    wf_attrib_id = _add_or_get_wf_attribute_type(name)
    insert_vals = insert(WorkflowAttribute).values(
        workflow_id=workflow_id, workflow_attribute_type_id=wf_attrib_id, value=value
    )

    upsert_stmt = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value, status="U"
    )

    DB.session.execute(upsert_stmt)
    DB.session.commit()


@finite_state_machine.route(
    "/workflow/<workflow_id>/workflow_attributes", methods=["PUT"]
)
def update_workflow_attribute(workflow_id: int) -> Any:
    """Update the attributes for a given workflow."""
    bind_to_logger(workflow_id=workflow_id)
    try:
        int(workflow_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    """ Add/update attributes for a workflow """
    data = request.get_json()
    logger.debug("Update attributes")
    attributes = data["workflow_attributes"]
    if attributes:
        for name, val in attributes.items():
            _upsert_wf_attribute(workflow_id, name, val)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow/<workflow_id>/set_resume", methods=["POST"])
def set_resume(workflow_id: int) -> Any:
    """Set resume on a workflow."""
    bind_to_logger(workflow_id=workflow_id)
    try:
        data = request.get_json()
        logger.info("Set resume for workflow")
        reset_running_jobs = bool(data["reset_running_jobs"])
        description = str(data["description"])
        name = str(data["name"])
        max_concurrently_running = int(data["max_concurrently_running"])
        workflow_attributes = data["workflow_attributes"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = (
        DB.session.query(Workflow)
        .from_statement(text(query))
        .params(workflow_id=workflow_id)
        .one()
    )

    # set mutable attribute
    workflow.description = description
    workflow.name = name
    workflow.max_concurrently_running = max_concurrently_running
    DB.session.commit()

    # trigger resume on active workflow run
    workflow.resume(reset_running_jobs)
    DB.session.commit()
    logger.info(f"Resume set for wf {workflow_id}")

    # update attributes
    if workflow_attributes:
        logger.info("Update attributes for workflow")
        _add_workflow_attributes(workflow.id, workflow_attributes)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow/<workflow_id>/is_resumable", methods=["GET"])
def workflow_is_resumable(workflow_id: int) -> Any:
    """Check if a workflow is in a resumable state."""
    bind_to_logger(workflow_id=workflow_id)
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = (
        DB.session.query(Workflow)
        .from_statement(text(query))
        .params(workflow_id=workflow_id)
        .one()
    )
    DB.session.commit()
    logger.info(f"Workflow is resumable: {workflow.is_resumable}")
    resp = jsonify(workflow_is_resumable=workflow.is_resumable)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "workflow/<workflow_id>/update_max_running", methods=["PUT"]
)
def update_max_running(workflow_id: int) -> Any:
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = request.get_json()
    bind_to_logger(workflow_id=workflow_id)
    logger.debug("Update workflow max running")
    try:
        new_limit = data["max_tasks"]
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    q = """
        UPDATE workflow
        SET max_concurrently_running = {new_limit}
        WHERE id = {workflow_id}
    """.format(
        new_limit=new_limit, workflow_id=workflow_id
    )

    res = DB.session.execute(q)
    DB.session.commit()

    if res.rowcount == 0:  # Return a warning message if no update was performed
        message = (
            f"No update performed for workflow ID {workflow_id}, max_concurrency is "
            f"{new_limit}"
        )
    else:
        message = f"Workflow ID {workflow_id} max concurrency updated to {new_limit}"

    resp = jsonify(message=message)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow/<workflow_id>/task_status_updates", methods=["POST"]
)
def get_task_by_status_only(workflow_id: int) -> Any:
    """Returns all tasks in the database that have the specified status.

    Args:
        workflow_id (int): the ID of the workflow.
    """
    bind_to_logger(workflow_id=workflow_id)
    data = request.get_json()
    logger.info("Get task by status")

    last_sync = data["last_sync"]
    swarm_tasks_tuples = data.get("swarm_tasks_tuples", [])

    # get time from db
    db_time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS t").fetchone()["t"]
    str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()

    if swarm_tasks_tuples:
        # Sample swarm_tasks_tuples: [(1, 'I')]
        swarm_task_ids = ",".join([str(task_id[0]) for task_id in swarm_tasks_tuples])
        swarm_tasks_tuples = [
            (int(task_id), str(status)) for task_id, status in swarm_tasks_tuples
        ]

        query_swarm_tasks_tuples = ""
        for task_id, status in swarm_tasks_tuples:
            query_swarm_tasks_tuples += f"({task_id},'{status}'),"
        # get rid of trailing comma on final line
        query_swarm_tasks_tuples = query_swarm_tasks_tuples[:-1]

        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = {workflow_id}
                AND (
                    (
                        task.id IN ({swarm_task_ids})
                        AND (task.id, status) NOT IN ({tuples})
                    )
                    OR status_date >= '{status_date}')
        """.format(
            workflow_id=workflow_id,
            swarm_task_ids=swarm_task_ids,
            tuples=query_swarm_tasks_tuples,
            status_date=last_sync,
        )
        logger.debug(query)
        rows = DB.session.query(Task).from_statement(text(query)).all()
    else:
        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = :workflow_id
                AND status_date >= :last_sync"""
        rows = (
            DB.session.query(Task)
            .from_statement(text(query))
            .params(workflow_id=workflow_id, last_sync=str(last_sync))
            .all()
        )

    DB.session.commit()
    task_dcts = [row.to_wire_as_swarm_task() for row in rows]
    logger.debug("task_dcts={}".format(task_dcts))
    resp = jsonify(task_dcts=task_dcts, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow_validation", methods=["POST"])
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

    task_list = ""
    for id in task_ids:
        task_list = task_list + str(id) + ","
    task_list = task_list[:-1]

    # execute query
    q = f"""
        SELECT
            distinct t.workflow_id, wf.status
        FROM task t
        INNER JOIN workflow wf ON t.workflow_id = wf.id
        WHERE t.id IN ({task_list})
    """
    res = DB.session.execute(q).fetchall()

    # Validate if all tasks are in the same workflow and the workflow status is dead
    if len(res) == 1 and res[0][1] in (
        Statuses.FAILED,
        Statuses.DONE,
        Statuses.ABORTED,
        Statuses.HALTED,
    ):
        validation = True
    else:
        validation = False

    resp = jsonify(validation=validation, workflow_status=res[0][1])
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow_status", methods=["GET"])
def get_workflow_status() -> Any:
    """Get the status of the workflow."""
    # initial params
    params = {}
    user_request = request.args.getlist("user")
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    workflow_request = request.args.getlist("workflow_id")
    bind_to_logger(user=user_request)
    logger.debug(f"Query for wf {workflow_request} status.")
    if workflow_request == "all":  # specifying all is equivalent to None
        workflow_request = []
    limit_request = request.args.getlist("limit")
    limit = None if len(limit_request) == 0 else limit_request[0]
    # anything less than 0 or non number will be treated as None
    try:
        if int(limit_request[0]) < 0:
            limit = None
    except ValueError:
        limit = None
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
        df = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS", "STATUS"]).agg(
            {"TASKS": "sum", "RETRIES": "sum"}
        )

        # pivot wide by task status
        tasks = df.pivot_table(
            values="TASKS",
            index=["WF_ID", "WF_NAME", "WF_STATUS"],
            columns="STATUS",
            fill_value=0,
        )
        for col in _cli_order:
            if col not in tasks.columns:
                tasks[col] = 0
        tasks = tasks[_cli_order]

        # aggregate again without status to get the totals by workflow
        retries = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS"]).agg(
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


@finite_state_machine.route("/workflow/<workflow_id>/workflow_tasks", methods=["GET"])
def get_workflow_tasks(workflow_id: int) -> Any:
    """Get the tasks for a given workflow."""
    params: Dict = {"workflow_id": workflow_id}
    bind_to_logger(workflow_id=workflow_id)
    limit_request = request.args.getlist("limit")
    limit = None if len(limit_request) == 0 else limit_request[0]
    # anything less than 0 or non number will be treated as None
    try:
        if int(limit_request[0]) < 0:
            limit = None
    except ValueError:
        limit = None
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


@finite_state_machine.route("/workflow/<workflow_id>/usernames", methods=["GET"])
def get_workflow_users(workflow_id: int) -> Any:
    """Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    bind_to_logger(workflow_id=workflow_id)
    logger.debug("Get associated users for workflow")

    query = """
        SELECT DISTINCT user
        FROM workflow_run
        WHERE workflow_run.workflow_id = {workflow_id}
    """.format(
        workflow_id=workflow_id
    )

    result = DB.session.execute(query)

    usernames = [row.user for row in result]
    logger.info(f"User names associated with workflow:\n{usernames}")
    resp = jsonify(usernames=usernames)

    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
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


@finite_state_machine.route(
    "/workflow/<workflow_id>/queued_tasks/<n_queued_tasks>", methods=["GET"]
)
def get_queued_jobs(workflow_id: int, n_queued_tasks: int) -> Any:
    """Returns oldest n tasks (or all tasks if total queued tasks < n) to be instantiated.

    Because the SGE can only qsub tasks at a certain rate, and we poll every 10 seconds, it
    does not make sense to return all tasks that are queued because only a subset of them can
    actually be instantiated.

    Args:
        workflow_id: id of workflow
        n_queued_tasks: number of tasks to queue
        last_sync (datetime): time since when to get tasks
    """
    # <usertablename>_<columnname>.

    # If we want to prioritize by task or workflow level it would be done in this query
    bind_to_logger(workflow_id=workflow_id)
    logger.info("Getting queued jobs for workflow")
    # queue_limit_query = """
    #     SELECT (
    #         SELECT
    #             max_concurrently_running
    #         FROM
    #             workflow
    #         WHERE
    #             id = :workflow_id
    #         ) - (
    #         SELECT
    #             count(*)
    #         FROM
    #             task
    #         WHERE
    #             task.workflow_id = :workflow_id
    #             AND task.status IN ("I", "R")
    #         )
    #     AS queue_limit
    # """
    # concurrency_limit = DB.session.execute(
    #     queue_limit_query, {"workflow_id": int(workflow_id)}
    # ).fetchone()[0]

    # # query if we aren't at the concurrency_limit
    # if concurrency_limit > 0:
    #     concurrency_limit = min(int(concurrency_limit), int(n_queued_tasks))

    tasks = (
        DB.session.query(Task)
        .options(joinedload(Task.task_resources))
        .options(joinedload(Task.array))
        .filter(Task.workflow_id == workflow_id, Task.status == "Q")
        .limit(int(n_queued_tasks))
        .all()
    )
    DB.session.commit()
    task_dcts = [t.to_wire_as_distributor_task() for t in tasks]
    logger.debug(f"Got the following queued tasks: {task_dcts}")
    resp = jsonify(task_dcts=task_dcts)
    return resp


@finite_state_machine.route(
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


@finite_state_machine.route("workflow/<workflow_id>/reset", methods=["PUT"])
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


@finite_state_machine.route(
    "workflow/<workflow_id>/fix_status_inconsitency", methods=["PUT"]
)
def fix_wf_inconsistency(workflow_id: int) -> Any:
    """Find wf in F with all tasks in D and fix them."""
    sql = "SELECT COUNT(*) as total FROM workflow"
    # the id to return to reaper as next start point
    total_wf = int(DB.session.execute(sql).fetchone()["total"])

    # move the starting row forward by 3000
    # if the starting row > max row, restart from 0
    # this way, we can get to the unfinished the wf later
    # without querying the whole db every time
    increase_step = 3000
    current_max_wf_id = int(workflow_id) + int(increase_step)
    if current_max_wf_id > total_wf:
        current_max_wf_id = 0

    # Update wf in F with all task in D to D
    # limit the query lines to 1k, which should finish <1s
    # and won't shock the db when reaper restarts
    sql = """UPDATE workflow
            SET status = "D"
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, count(s), sum(s)
                    FROM
                        (SELECT workflow.id, (case when task.status="D" then 1 else 0 end) as s
                        FROM workflow, task
                        WHERE workflow.id > {wfid1}
                        AND workflow.id <= {wfid2}
                        AND workflow.status='F'
                        AND workflow.id=task.workflow_id) t
                        GROUP BY id
                        HAVING count(s) = sum(s) ) tt
            )
            """.format(
        wfid1=workflow_id, wfid2=int(workflow_id) + increase_step
    )

    DB.session.execute(sql)
    DB.session.commit()

    resp = jsonify({"wfid": current_max_wf_id})
    resp.status_code = StatusCodes.OK
    return resp
