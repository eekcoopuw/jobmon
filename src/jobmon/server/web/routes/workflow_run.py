"""Routes for WorkflowRuns"""
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.server_side_exception import InvalidUsage

from sqlalchemy.sql import text

from . import jobmon_client, jobmon_scheduler, jobmon_swarm


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
    app.logger = app.logger.bind(workflow_run_id=workflow_run.id)
    app.logger.info(f"Add workflow_run:{workflow_run.id} for workflow: {wid}.")
    resp = jsonify(workflow_run_id=workflow_run.id)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run/<workflow_run_id>/link', methods=['POST'])
def link_workflow_run(workflow_run_id: int):
    """Link this workflow run to a workflow."""
    try:
        data = request.get_json()
        app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
        next_report_increment = float(data["next_report_increment"])
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
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
    app.logger.debug(f"Got wf for wfr {workflow_run_id}: {workflow.id}")
    DB.session.refresh(workflow, with_for_update=True)

    # check if any workflow run is in linked state.
    # if not any linked, proceed.
    app.logger.debug("Check if any wfr is in linked state; otherwise, link.")
    current_wfr = workflow.link_workflow_run(workflow_run, next_report_increment)
    app.logger.debug("WF linked")
    DB.session.commit()  # release lock
    resp = jsonify(current_wfr=current_wfr)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
def client_terminate_workflow_run(workflow_run_id: int):
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
    app.logger.debug(f"Error logged for {workflow_run_id}")

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
    app.logger.debug(f"Job instance status updated for {workflow_run_id}")
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
def client_log_workflow_run_heartbeat(workflow_run_id: int):
    """Log a heartbeat on behalf of the workflow run to show that the client side is still
    alive.
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    data = request.get_json()
    app.logger.debug(f"WFR {workflow_run_id} heartbeat data")

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


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/update_status', methods=['PUT'])
def log_workflow_run_status_update(workflow_run_id: int):
    """Update the status of the workflow run."""
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    data = request.get_json()

    workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()
    workflow_run.transition(data["status"])
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/aborted/<aborted_seconds>',
                    methods=['PUT'])
def get_run_status_and_latest_task(workflow_run_id: int, aborted_seconds: int):
    """If the last task was more than 2 minutes ago, transition wfr to A state
    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)

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
    wfr = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_id=workflow_run_id, aborted_seconds=aborted_seconds
    ).one_or_none()
    DB.session.commit()

    if wfr is not None:
        wfr.transition(WorkflowRunStatus.ABORTED)
        DB.session.commit()
        aborted = True
    else:
        aborted = False
    resp = jsonify(was_aborted=aborted)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def log_wfr_heartbeat(workflow_run_id: int):
    """Log a workflow_run as being responsive, with a heartbeat
    Args:

        workflow_run_id: id of the workflow_run to log
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    app.logger.debug(f"Log heartbeat for wfr {workflow_run_id}")
    params = {"workflow_run_id": int(workflow_run_id)}
    query = """
        UPDATE workflow_run
        SET heartbeat_date = CURRENT_TIMESTAMP()
        WHERE id = :workflow_run_id
    """
    DB.session.execute(query, params)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def scheduler_log_workflow_run_heartbeat(workflow_run_id: int):
    """Log a heartbeat on behalf of the workflow run to show that the client side is still
    alive.
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    data = request.get_json()

    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()

    try:
        workflow_run.heartbeat(data["next_report_increment"])
        DB.session.commit()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition:
        DB.session.rollback()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat rolled back")

    resp = jsonify(message=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/lost_workflow_run', methods=['GET'])
def get_lost_workflow_runs():
    """Return all workflow runs that are currently in the specified state."""
    statuses = request.args.getlist('status')
    version = request.args.get('version')
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
    workflow_runs = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_status=statuses, version=version
    ).all()
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/reap', methods=['PUT'])
def reap_workflow_run(workflow_run_id: int):
    """If the last task was more than 2 minutes ago, transition wfr to A state
    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    query = """
        SELECT
            workflow_run.*
        FROM workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
            and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
    """
    wfr = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_id=workflow_run_id
    ).one_or_none()
    DB.session.commit()

    try:
        wfr.reap()
        DB.session.commit()
        status = wfr.status
    except (InvalidStateTransition, AttributeError) as e:
        # this branch handles race condition or case where no wfr was returned
        app.logger.debug(f"Unable to reap workflow_run {wfr.id}: {e}")
        status = ""
    resp = jsonify(status=status)
    resp.status_code = StatusCodes.OK
    return resp
