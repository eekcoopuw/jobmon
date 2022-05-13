"""Routes for WorkflowRuns."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy.sql import text
from werkzeug.local import LocalProxy

from jobmon.exceptions import InvalidStateTransition
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.server_side_exception import InvalidUsage


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route("/workflow_run", methods=["POST"])
def add_workflow_run() -> Any:
    """Add a workflow run to the db."""
    try:
        data = request.get_json()
        wid = data["workflow_id"]
        int(wid)
        bind_to_logger(workflow_id=wid)
        logger.info(f"Add wfr for workflow_id:{wid}.")

    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    workflow_run = WorkflowRun(
        workflow_id=wid,
        user=data["user"],
        jobmon_version=data["jobmon_version"],
        status=WorkflowRunStatus.REGISTERED,
    )
    DB.session.add(workflow_run)
    DB.session.commit()
    logger.info(f"Add workflow_run:{workflow_run.id} for workflow.")
    resp = jsonify(workflow_run_id=workflow_run.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow_run/<workflow_run_id>/link", methods=["POST"])
def link_workflow_run(workflow_run_id: int) -> Any:
    """Link this workflow run to a workflow."""
    try:
        data = request.get_json()
        bind_to_logger(workflow_run_id=workflow_run_id)
        logger.info("Linking workflow_run")
        next_report_increment = float(data["next_report_increment"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    logger.info(f"Query wfr with {workflow_run_id}")
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
    """
    workflow_run = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_id=workflow_run_id)
        .one()
    )

    # refresh with lock in case other workflow run is trying to progress
    workflow = workflow_run.workflow
    logger.info(f"Got wf for wfr {workflow_run_id}: {workflow}")
    DB.session.refresh(workflow, with_for_update=True)

    # check if any workflow run is in linked state.
    # if not any linked, proceed.
    logger.debug("Check if any wfr is in linked state; otherwise, link.")
    current_wfr = workflow.link_workflow_run(workflow_run, next_report_increment)
    logger.info("WF linked")
    DB.session.commit()  # release lock
    resp = jsonify(current_wfr=current_wfr)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/terminate_task_instances", methods=["PUT"]
)
def terminate_workflow_run(workflow_run_id: int) -> Any:
    """Terminate a workflow run and get its tasks in order."""
    bind_to_logger(workflow_run_id=workflow_run_id)
    logger.info("Terminate workflow_run")
    try:
        int(workflow_run_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()

    if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
        states = [TaskStatus.LAUNCHED]
    else:
        states = [TaskStatus.LAUNCHED, TaskInstanceStatus.RUNNING]

    # update task instance states
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
    DB.session.execute(
        update_task_instance, {"workflow_run_id": workflow_run_id, "states": states}
    )
    DB.session.flush()
    logger.debug(f"Job instance status updated for {workflow_run_id}")

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
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task_instance.status = 'K'
    """
    DB.session.execute(
        log_errors, {"workflow_run_id": int(workflow_run_id), "states": states}
    )
    DB.session.commit()
    logger.debug(f"Error logged for {workflow_run_id}")

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow_run_status", methods=["GET"])
def get_active_workflow_runs() -> Any:
    """Return all workflow runs that are currently in the specified state."""
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.status in :workflow_run_status
    """
    workflow_runs = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_status=request.args.getlist("status"))
        .all()
    )
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/log_heartbeat", methods=["POST"]
)
def log_workflow_run_heartbeat(workflow_run_id: int) -> Any:
    """Log a heartbeat for the workflow run to show that the client side is still alive."""
    bind_to_logger(workflow_run_id=workflow_run_id)
    data = request.get_json()
    logger.debug(f"WFR {workflow_run_id} heartbeat data")

    workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()

    try:
        workflow_run.heartbeat(data["next_report_increment"], data["status"])
        DB.session.commit()
        logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition as e:
        DB.session.rollback()
        logger.debug(f"wfr {workflow_run_id} heartbeat rolled back, reason: {e}")

    resp = jsonify(status=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/update_status", methods=["PUT"]
)
def log_workflow_run_status_update(workflow_run_id: int) -> Any:
    """Update the status of the workflow run."""
    bind_to_logger(workflow_run_id=workflow_run_id)
    data = request.get_json()
    logger.info(f"Log status update for workflow_run_id:{workflow_run_id}.")

    workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()
    status = data["status"]
    try:
        workflow_run.transition(status)
        DB.session.commit()
    except InvalidStateTransition:
        DB.session.rollback()
        # Return the original status
        status = workflow_run.status

    resp = jsonify(status=status)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/aborted/<aborted_seconds>", methods=["PUT"]
)
def get_run_status_and_latest_task(workflow_run_id: int, aborted_seconds: int) -> Any:
    """If the last task was more than 2 minutes ago, transition wfr to A state.

    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    bind_to_logger(workflow_run_id=workflow_run_id)

    query = """
        SELECT
            workflow_run.*,
            TIMESTAMPDIFF(
                SECOND, workflow_run.status_date, CURRENT_TIMESTAMP
            ) AS workflow_created,
            TIMESTAMPDIFF(
                SECOND, max(task.status_date), CURRENT_TIMESTAMP
            ) AS task_created
        FROM workflow_run
        JOIN workflow ON workflow_run.workflow_id = workflow.id
        LEFT JOIN task ON workflow_run.workflow_id = task.workflow_id
        WHERE
            workflow_run.id = :workflow_run_id
            AND workflow.status != 'S'
        HAVING
            (
                workflow_created > :aborted_seconds
                AND task_created > :aborted_seconds
            )
            OR (workflow_created > :aborted_seconds and task_created is NULL)
    """
    wfr = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_id=workflow_run_id, aborted_seconds=aborted_seconds)
        .one_or_none()
    )
    DB.session.commit()

    if wfr is not None:
        logger.info(f"Transit wfr {workflow_run_id} to ABORTED")
        wfr.transition(WorkflowRunStatus.ABORTED)
        DB.session.commit()
        aborted = True
    else:
        logger.info(f"Do not transit wfr {workflow_run_id} to ABORTED")
        aborted = False
    resp = jsonify(was_aborted=aborted)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/lost_workflow_run", methods=["GET"])
def get_lost_workflow_runs() -> Any:
    """Return all workflow runs that are currently in the specified state."""
    statuses = request.args.getlist("status")
    version = request.args.get("version")
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.status in :workflow_run_status
            and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
            and workflow_run.jobmon_version = :version
    """
    workflow_runs = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_status=statuses, version=version)
        .all()
    )
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/workflow_run/<workflow_run_id>/reap", methods=["PUT"])
def reap_workflow_run(workflow_run_id: int) -> Any:
    """If the last task was more than 2 minutes ago, transition wfr to A state.

    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    bind_to_logger(workflow_run_id=workflow_run_id)
    logger.info(f"Reap wfr: {workflow_run_id}")
    query = """
        SELECT
            workflow_run.*
        FROM workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
            and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
    """
    wfr = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_id=workflow_run_id)
        .one_or_none()
    )
    DB.session.commit()

    try:
        wfr.reap()
        DB.session.commit()
        status = wfr.status
    except (InvalidStateTransition, AttributeError) as e:
        # this branch handles race condition or case where no wfr was returned
        logger.debug(f"Unable to reap workflow_run {wfr.id}: {e}")
        status = ""
    resp = jsonify(status=status)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/sync_status", methods=["POST"]
)
def task_instances_status_check(workflow_run_id: int) -> Any:
    """Sync status of given task intance IDs."""
    data = request.get_json()
    task_instance_ids_list = data["task_instance_ids"]
    status = data["status"]

    # get time from db
    db_time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS t").fetchone()["t"]
    str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()

    return_dict = dict()
    if len(task_instance_ids_list) > 0:
        task_instance_ids = ",".join(f"{x}" for x in task_instance_ids_list)

        # Filters for
        # 1) instances that have changed out of the declared status
        # 2) instances that have changed into the declared status
        filters = f"""
            (id in ({task_instance_ids}) AND status != '{status}') OR
            (id not in ({task_instance_ids}) AND status = '{status}')
        """
    else:
        filters = f"""status = '{status}'"""

    # someday, when the distributor is centralized. we will remove the workflow_run_id from
    # this query, but not today
    sql = f"""
        SELECT id, status
        FROM task_instance
        WHERE
            workflow_run_id = {workflow_run_id} AND
            ({filters})
        """
    rows = DB.session.execute(sql).fetchall()
    if rows:
        for row in rows:
            return_dict[row["id"]] = row["status"]

    resp = jsonify(status_updates=return_dict, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/set_status_for_triaging", methods=["POST"]
)
def set_status_for_triaging(workflow_run_id: int) -> Any:
    """2 triaging related status sets

    Query all task instances that are submitted to distributor or running which haven't
    reported as alive in the allocated time, and set them for Triaging(from Running)
    and Kill_self(from Launched).
    """
    bind_to_logger(workflow_run_id=workflow_run_id)
    logger.info(f"Set to triaging those overdue tis for wfr {workflow_run_id}")
    params = {
        "running_status": TaskInstanceStatus.RUNNING,
        "triaging_status": TaskInstanceStatus.TRIAGING,
        "launched_status": TaskInstanceStatus.LAUNCHED,
        "workflow_run_id": workflow_run_id,
    }
    # Update all running tasks that have a report by date less than now to triaging
    sql = """
        UPDATE task_instance
        SET status = :triaging_status,
            status_date = CURRENT_TIMESTAMP()
        WHERE
            workflow_run_id = :workflow_run_id
            AND status = :running_status
            AND report_by_date <= CURRENT_TIMESTAMP()
    """
    DB.session.execute(sql, params)
    DB.session.commit()

    # Find all the TaskIntances that should be moved to KILL_SELF state and then loop through
    # them and update the status. Do this instead of a bulk update to avoid a deadlock. The
    # deadlock is non-local, it happens when run simultaneously with the
    # "/task_instance/log_report_by/batch" route. See GBDSCI-4582 for more details.
    sql = """
        SELECT id
        FROM task_instance
        WHERE
            workflow_run_id = :workflow_run_id
            AND status = :launched_status
            AND report_by_date <= CURRENT_TIMESTAMP()
    """
    ti_results = DB.session.execute(sql, params).fetchall()
    DB.session.commit()

    if ti_results is None or len(ti_results) == 0:
        logger.info(f"No TaskInstances that need to be moved to KILL_SELF state.")
    else:
        ids = ",".join([str(row['id']) for row in ti_results])
        for ti in ids:
            update_sql = f"""UPDATE task_instance
                            SET status = :kill_self_status
                            WHERE id = :ti_id
                            """
            DB.session.execute(update_sql, {"kill_self_status": TaskInstanceStatus.KILL_SELF,
                                            "ti_id": ti})
            DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
