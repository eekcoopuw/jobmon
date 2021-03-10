"""Routes for Scheduler component of client architecture."""
import os
import sys
from http import HTTPStatus as StatusCodes
from typing import Optional

from flask import Blueprint, current_app as app, jsonify, request

from jobmon.constants import QsubAttribute
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.server_side_exception import ServerError

import sqlalchemy
from sqlalchemy.sql import func, text


jobmon_scheduler = Blueprint("jobmon_scheduler", __name__)


@jobmon_scheduler.before_request
def log_request_info():
    """Add blueprint to logger."""
    app.logger = app.logger.bind(blueprint=jobmon_scheduler.name)
    app.logger.debug("starting route execution")


@jobmon_scheduler.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating that this responder
    is in fact listening.
    """
    app.logger.info(f"{os.getpid()}: {__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route("/time", methods=['GET'])
def get_pst_now():
    """Get the current time according to the database."""
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route("/health", methods=['GET'])
def health():
    """
    Test connectivity to the database, return 200 if everything is ok
    Defined in each module with a different route, so it can be checked individually
    """
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    # Assume that if we got this far without throwing an exception, we should be online
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow/<workflow_id>/queued_tasks/<n_queued_tasks>',
                        methods=['GET'])
def get_queued_jobs(workflow_id: int, n_queued_tasks: int):
    """Returns oldest n tasks (or all tasks if total queued tasks < n) to be
    instantiated. Because the SGE can only qsub tasks at a certain rate, and we
    poll every 10 seconds, it does not make sense to return all tasks that are
    queued because only a subset of them can actually be instantiated
    Args:
        workflow_id: id of workflow
        n_queued_tasks: number of tasks to queue
        last_sync (datetime): time since when to get tasks
    """
    # <usertablename>_<columnname>.

    # If we want to prioritize by task or workflow level it would be done in this query
    app.logger = app.logger.bind(workflow_id=workflow_id)
    queue_limit_query = """
        SELECT (
            SELECT
                max_concurrently_running
            FROM
                workflow
            WHERE
                id = :workflow_id
            ) - (
            SELECT
                count(*)
            FROM
                task
            WHERE
                task.workflow_id = :workflow_id
                AND task.status IN ("I", "R")
            )
        AS queue_limit
    """
    concurrency_limit = DB.session.execute(
        queue_limit_query, {'workflow_id': int(workflow_id)}
    ).fetchone()[0]
    print(concurrency_limit)

    # query if we aren't at the concurrency_limit
    if concurrency_limit > 0:
        concurrency_limit = min(int(concurrency_limit), int(n_queued_tasks))
        task_query = """
            SELECT
                task.id AS task_id,
                task.workflow_id AS task_workflow_id,
                task.node_id AS task_node_id,
                task.task_args_hash AS task_task_args_hash,
                task.name AS task_name,
                task.command AS task_command,
                task.status AS task_status,
                executor_parameter_set.max_runtime_seconds AS
                    executor_parameter_set_max_runtime_seconds,
                executor_parameter_set.context_args AS executor_parameter_set_context_args,
                executor_parameter_set.resource_scales AS
                    executor_parameter_set_resource_scales,
                executor_parameter_set.queue AS executor_parameter_set_queue,
                executor_parameter_set.num_cores AS executor_parameter_set_num_cores,
                executor_parameter_set.m_mem_free AS executor_parameter_set_m_mem_free,
                executor_parameter_set.j_resource AS executor_parameter_set_j_resource,
                executor_parameter_set.hard_limits AS executor_parameter_set_hard_limits
            FROM
                task
            JOIN
                executor_parameter_set
                ON task.executor_parameter_set_id = executor_parameter_set.id
            JOIN
                workflow
                ON task.workflow_id = workflow.id
            WHERE
                task.workflow_id = :workflow_id
                AND task.status = "Q"
            LIMIT :concurrency_limit
        """

        tasks = DB.session.query(Task).from_statement(text(task_query)).params(
            workflow_id=workflow_id,
            concurrency_limit=concurrency_limit
        ).all()
        DB.session.commit()
        task_dcts = [t.to_wire_as_executor_task() for t in tasks]
    else:
        task_dcts = []
    resp = jsonify(task_dcts=task_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow_run/<workflow_run_id>/get_suspicious_task_instances',
                        methods=['GET'])
def get_suspicious_task_instances(workflow_run_id: int):
    """Query all task instances that are submitted to executor or running which haven't
    reported as alive in the allocated time.
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    query = """
        SELECT
            task_instance.id, task_instance.workflow_run_id,
            task_instance.executor_id
        FROM
            task_instance
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task_instance.status in :active_tasks
            AND task_instance.report_by_date <= CURRENT_TIMESTAMP()
    """
    rows = DB.session.query(TaskInstance).from_statement(text(query)).params(
        active_tasks=[TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                      TaskInstanceStatus.RUNNING],
        workflow_run_id=workflow_run_id
    ).all()
    DB.session.commit()
    resp = jsonify(task_instances=[ti.to_wire_as_executor_task_instance()
                                   for ti in rows])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow_run/<workflow_run_id>/get_task_instances_to_terminate',
                        methods=['GET'])
def get_task_instances_to_terminate(workflow_run_id: int):
    """Get the task instances for a given workflow run that need to be terminated."""
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id
    ).one()

    if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
        task_instance_states = [TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR]
    if workflow_run.status == WorkflowRunStatus.COLD_RESUME:
        task_instance_states = [TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                                TaskInstanceStatus.RUNNING]

    query = """
        SELECT
            task_instance.id, task_instance.workflow_run_id,
            task_instance.executor_id
        FROM
            task_instance
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task_instance.status in :task_instance_states
    """
    rows = DB.session.query(TaskInstance).from_statement(text(query)).params(
        task_instance_states=task_instance_states,
        workflow_run_id=workflow_run_id
    ).all()
    DB.session.commit()
    resp = jsonify(task_instances=[ti.to_wire_as_executor_task_instance()
                                   for ti in rows])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
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
        workflow_run.heartbeat(data["next_report_increment"])
        DB.session.commit()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition:
        DB.session.rollback()
        app.logger.debug(f"wfr {workflow_run_id} heartbeat rolled back")

    resp = jsonify(message=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/workflow_run/<workflow_run_id>/log_executor_report_by',
                        methods=['POST'])
def log_executor_report_by(workflow_run_id: int):
    """Log the next report by date and time."""
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    data = request.get_json()
    params = {"workflow_run_id": int(workflow_run_id)}
    for key in ["next_report_increment", "executor_ids"]:
        params[key] = data[key]

    if params["executor_ids"]:
        query = """
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE
                workflow_run_id = :workflow_run_id
                AND executor_id in :executor_ids
        """
        DB.session.execute(query, params)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/task_instance', methods=['POST'])
def add_task_instance():
    """Add a task_instance to the database

    Args:
        task_id (int): unique id for the task
        executor_type (str): string name of the executor type used
    """
    try:
        data = request.get_json()
        task_id = data['task_id']
        app.logger = app.logger.bind(task_id=task_id)
        # query task
        task = DB.session.query(Task).filter_by(id=task_id).first()
        DB.session.commit()

        # create task_instance from task parameters
        task_instance = TaskInstance(
            workflow_run_id=data["workflow_run_id"],
            executor_type=data['executor_type'],
            task_id=data['task_id'],
            executor_parameter_set_id=task.executor_parameter_set_id
        )
        DB.session.add(task_instance)
        DB.session.commit()
        task_instance.task.transition(TaskStatus.INSTANTIATED)
        DB.session.commit()
        resp = jsonify(task_instance=task_instance.to_wire_as_executor_task_instance())
        resp.status_code = StatusCodes.OK
        return resp
    except InvalidStateTransition as e:
        # Handles race condition if the task is already instantiated state
        if task_instance.task.status == TaskStatus.INSTANTIATED:
            msg = ("Caught InvalidStateTransition. Not transitioning task "
                   "{}'s task_instance_id {} from I to I"
                   .format(data['task_id'], task_instance.id))
            app.logger.warning(msg)
            DB.session.commit()
            resp = jsonify(
                task_instance=task_instance.to_wire_as_executor_task_instance())
            resp.status_code = StatusCodes.OK
            return resp
        else:
            raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                              status_code=500) from e


@jobmon_scheduler.route('/task_instance/<task_instance_id>/log_no_executor_id',
                        methods=['POST'])
def log_no_executor_id(task_instance_id: int):
    """Log a task_instance_id that did not get an executor_id upon submission."""
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    data = request.get_json()
    app.logger.debug(f"Log NO EXECUTOR ID for TI {task_instance_id}."
                     f"Data {data['executor_id']}")
    app.logger.debug("Add TI for task ")

    if data['executor_id'] == QsubAttribute.NO_EXEC_ID:
        app.logger.info("Qsub was unsuccessful and caused an exception")
    else:
        app.logger.info("Qsub may have run, but the sge job id could not be parsed"
                        " from the qsub response so no executor id can be assigned"
                        " at this time")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(ti, TaskInstanceStatus.NO_EXECUTOR_ID)
    ti.executor_id = data['executor_id']
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/task_instance/<task_instance_id>/log_executor_id', methods=['POST'])
def log_executor_id(task_instance_id: int):
    """Log a task_instance's executor id
    Args:

        task_instance_id: id of the task_instance to log
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    data = request.get_json()
    app.logger.debug(f"Log EXECUTOR ID for TI {task_instance_id}. Data {data}")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(
        ti, TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    ti.executor_id = data['executor_id']
    ti.report_by_date = func.ADDTIME(
        func.now(),
        func.SEC_TO_TIME(data["next_report_increment"]))
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/task_instance/<task_instance_id>/log_known_error', methods=['POST'])
def log_known_error(task_instance_id: int):
    """Log a task_instance as errored
    Args:
        task_instance_id (int): id for task instance
        data:
        oom_killed: whether or not given job errored due to an oom-kill event
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    data = request.get_json()
    error_state = data['error_state']
    error_message = data['error_message']
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    app.logger.debug(f"Log ERROR for TI:{task_instance_id}. Data: {data}")

    query = """
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
    """
    ti = DB.session.query(TaskInstance).from_statement(text(query)).params(
        task_instance_id=task_instance_id
    ).one_or_none()

    try:
        resp = _log_error(ti, error_state, error_message, executor_id,
                          nodename)
    except sqlalchemy.exc.OperationalError:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ti, error_state, new_msg, executor_id, nodename)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route('/task_instance/<task_instance_id>/log_unknown_error',
                        methods=['POST'])
def log_unknown_error(task_instance_id: int):
    """Log a task_instance as errored
    Args:
        task_instance_id (int): id for task instance
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    data = request.get_json()
    error_state = data['error_state']
    error_message = data['error_message']
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    app.logger.debug(f"Log ERROR for TI:{task_instance_id}. Data: {data}")

    query = """
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
            AND task_instance.report_by_date <= CURRENT_TIMESTAMP()
    """
    ti = DB.session.query(TaskInstance).from_statement(text(query)).params(
        task_instance_id=task_instance_id
    ).one_or_none()

    # make sure the task hasn't logged a new heartbeat since we began
    # reconciliation
    if ti is not None:
        try:
            resp = _log_error(ti, error_state, error_message, executor_id,
                              nodename)
        except sqlalchemy.exc.OperationalError:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(ti, error_state, new_msg, executor_id, nodename)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


# ############################ HELPER FUNCTIONS ###############################
def _update_task_instance_state(task_instance: TaskInstance, status_id: str):
    """Advance the states of task_instance and it's associated Task,
    return any messages that should be published based on
    the transition

    Args:
        task_instance (obj) object of time models.TaskInstance
        status_id (int): id of the status to which to transition
    """
    response = ""
    try:
        task_instance.transition(status_id)
    except InvalidStateTransition:
        if task_instance.status == status_id:
            # It was already in that state, just log it
            msg = f"Attempting to transition to existing state." \
                  f"Not transitioning task, tid= " \
                  f"{task_instance.id} from {task_instance.status} to " \
                  f"{status_id}"
            app.logger.warning(msg)
        else:
            # Tried to move to an illegal state
            msg = f"Illegal state transition. Not transitioning task, " \
                  f"tid={task_instance.id}, from {task_instance.status} to " \
                  f"{status_id}"
            app.logger.error(msg)
    except KillSelfTransition:
        msg = f"kill self, cannot transition tid={task_instance.id}"
        app.logger.warning(msg)
        response = "kill self"
    except Exception as e:
        msg = f"General exception in _update_task_instance_state, " \
              f"jid {task_instance}, transitioning to {task_instance}. " \
              f"Not transitioning task. {e}"
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e

    return response


def _log_error(ti: TaskInstance, error_state: int, error_msg: str,
               executor_id: Optional[int] = None,
               nodename: Optional[str] = None):
    if nodename is not None:
        ti.nodename = nodename
    if executor_id is not None:
        ti.executor_id = executor_id

    try:
        error = TaskInstanceErrorLog(task_instance_id=ti.id,
                                     description=error_msg)
        DB.session.add(error)
        msg = _update_task_instance_state(ti, error_state)
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        DB.session.rollback()
        app.logger.warning(str(e))
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e

    return resp


@jobmon_scheduler.route('/task_instance/<executor_id>/maxpss/<maxpss>', methods=['POST'])
def set_maxpss(executor_id: int, maxpss: int):
    """
    Route to set maxpss of a job instance
    :param executor_id: sge execution id
    :return:
    """
    app.logger = app.logger.bind(executor_id=executor_id)
    try:
        sql = f"UPDATE task_instance SET maxpss={maxpss} WHERE executor_id={executor_id}"
        DB.session.execute(sql)
        DB.session.commit()
        resp = jsonify(message=None)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        msg = "Error updating maxpss for execution id {eid}: {error}".format(eid=executor_id,
                                                                             error=str(e))
        app.logger.error(msg)
        raise ServerError(f"Unexpected Jobmon Server Error {sys.exc_info()[0]} in "
                          f"{request.path}", status_code=500) from e
