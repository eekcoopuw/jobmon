"""Routes for TaskInstances."""
from functools import partial
from http import HTTPStatus as StatusCodes
import sys
from typing import Any, Optional

from flask import jsonify, request
import sqlalchemy
from sqlalchemy.sql import func, text
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import (
    InvalidStateTransition,
    KillSelfTransition,
)
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.server_side_exception import ServerError


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/kill_self", methods=["GET"]
)
def kill_self(task_instance_id: int) -> Any:
    """Check a task instance's status to see if it needs to kill itself (state W, or L)."""
    bind_to_logger(task_instance_id=task_instance_id)
    logger.debug(f"Checking whether ti {task_instance_id} should commit suicide.")
    kill_statuses = TaskInstance.kill_self_states
    query = """
        SELECT
            task_instance.id
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
            AND task_instance.status in :statuses
    """
    should_kill = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(task_instance_id=task_instance_id, statuses=kill_statuses)
        .one_or_none()
    )
    logger.info(f"ti {task_instance_id} should_kill: {should_kill}")
    if should_kill is not None:
        resp = jsonify(should_kill=True)
        logger.info(f"ti {task_instance_id} should_kill: {should_kill}")
    else:
        resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_running", methods=["POST"]
)
def log_running(task_instance_id: int) -> Any:
    """Log a task_instance as running.

    Args:
        task_instance_id: id of the task_instance to log as running
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    logger.info(f"Log running for ti {task_instance_id}")
    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(ti, TaskInstanceStatus.RUNNING)
    if data.get("distributor_id", None) is not None:
        ti.distributor_id = data["distributor_id"]
    if data.get("nodename", None) is not None:
        ti.nodename = data["nodename"]
    ti.process_group_id = data["process_group_id"]
    ti.report_by_date = func.ADDTIME(
        func.now(), func.SEC_TO_TIME(data["next_report_increment"])
    )
    DB.session.commit()

    t = DB.session.query(Task).filter_by(id=ti.task_id).one()

    resp = jsonify(message=msg, command=t.command)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_report_by", methods=["POST"]
)
def log_ti_report_by(task_instance_id: int) -> Any:
    """Log a task_instance as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    logger.debug(f"Log report_by for TI {task_instance_id}.")

    distributor_id = data.get("distributor_id", None)
    params = {}
    params["next_report_increment"] = data["next_report_increment"]
    params["task_instance_id"] = task_instance_id
    if distributor_id is not None:
        params["distributor_id"] = distributor_id
        query = """
                UPDATE task_instance
                SET report_by_date = ADDTIME(
                    CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment)),
                    distributor_id = :distributor_id
                WHERE task_instance.id = :task_instance_id"""
    else:
        query = """
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE task_instance.id = :task_instance_id"""

    DB.session.execute(query, params)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/log_report_by/batch", methods=["POST"]
)
def log_ti_report_by_batch() -> Any:
    """Log task_instances as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
    """
    data = request.get_json()
    tis = data.get("task_instance_ids", None)

    logger.debug(f"Log report_by for TI {tis}.")
    if tis:
        query = f"""
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE task_instance.id in str(tis).replace("[", "(").replace("]", ")")"""

        DB.session.execute(query)
        DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_usage", methods=["POST"]
)
def log_usage(task_instance_id: int) -> Any:
    """Log the usage stats of a task_instance.

    Args:
        task_instance_id: id of the task_instance to log done
        usage_str (str, optional): stats such as maxrss, etc
        wallclock (str, optional): wallclock of running job
        maxrss (str, optional): max resident set size mem used
        maxpss (str, optional): max proportional set size mem used
        cpu (str, optional): cpu used
        io (str, optional): io used
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    if data.get("maxrss", None) is None:
        data["maxrss"] = "-1"

    logger.debug(
        f"usage_str is {data.get('usage_str', None)}, "
        f"wallclock is {data.get('wallclock', None)}, "
        f"maxrss is {data.get('maxrss', None)}, "
        f"maxpss is {data.get('maxpss', None)}, "
        f"cpu is {data.get('cpu', None)}, "
        f" io is {data.get('io', None)}"
    )

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    if data.get("usage_str", None) is not None:
        ti.usage_str = data["usage_str"]
    if data.get("wallclock", None) is not None:
        ti.wallclock = data["wallclock"]
    if data.get("maxrss", None) is not None:
        ti.maxrss = data["maxrss"]
    if data.get("maxpss", None) is not None:
        ti.maxpss = data["maxpss"]
    if data.get("cpu", None) is not None:
        ti.cpu = data["cpu"]
    if data.get("io", None) is not None:
        ti.io = data["io"]
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_done", methods=["POST"]
)
def log_done(task_instance_id: int) -> Any:
    """Log a task_instance as done.

    Args:
        task_instance_id: id of the task_instance to log done
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    if data.get("distributor_id", None) is not None:
        ti.distributor_id = data["distributor_id"]
    if data.get("nodename", None) is not None:
        ti.nodename = data["nodename"]
    msg = _update_task_instance_state(ti, TaskInstanceStatus.DONE)
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_error_worker_node", methods=["POST"]
)
def log_error_worker_node(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (str): id of the task_instance to log done
        error_message (str): message to log as error
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()

    try:
        resp = _log_error(ti, error_state, error_message, distributor_id, nodename)
        return resp
    except sqlalchemy.exc.OperationalError:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ti, error_state, new_msg, distributor_id, nodename)
        return resp


@finite_state_machine.route("/task/<task_id>/most_recent_ti_error", methods=["GET"])
def get_most_recent_ti_error(task_id: int) -> Any:
    """Route to determine the cause of the most recent task_instance's error.

    Args:
        task_id (int): the ID of the task.

    Return:
        error message
    """
    bind_to_logger(task_id=task_id)
    logger.info(f"Getting most recent ji error for ti {task_id}")
    query = """
        SELECT
            tiel.*
        FROM
            task_instance ti
        JOIN
            task_instance_error_log tiel
            ON ti.id = tiel.task_instance_id
        WHERE
            ti.task_id = :task_id
        ORDER BY
            ti.id desc, tiel.id desc
        LIMIT 1"""
    ti_error = (
        DB.session.query(TaskInstanceErrorLog)
        .from_statement(text(query))
        .params(task_id=task_id)
        .one_or_none()
    )
    DB.session.commit()
    if ti_error is not None:
        resp = jsonify(
            {
                "error_description": ti_error.description,
                "task_instance_id": ti_error.task_instance_id,
            }
        )
    else:
        resp = jsonify({"error_description": "", "task_instance_id": None})
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/task_instance_error_log", methods=["GET"]
)
def get_task_instance_error_log(task_instance_id: int) -> Any:
    """Route to return all task_instance_error_log entries of the task_instance_id.

    Args:
        task_instance_id (int): ID of the task instance

    Return:
        jsonified task_instance_error_log result set
    """
    bind_to_logger(task_instance_id=task_instance_id)
    logger.info(f"Getting task instance error log for ti {task_instance_id}")
    query = """
        SELECT
            tiel.id, tiel.error_time, tiel.description
        FROM
            task_instance_error_log tiel
        WHERE
            tiel.task_instance_id = :task_instance_id
        ORDER BY
            tiel.id ASC"""
    ti_errors = (
        DB.session.query(TaskInstanceErrorLog)
        .from_statement(text(query))
        .params(task_instance_id=task_instance_id)
        .all()
    )
    DB.session.commit()
    resp = jsonify(task_instance_error_log=[tiel.to_wire() for tiel in ti_errors])
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/get_suspicious_task_instances", methods=["GET"]
)
def get_suspicious_task_instances(workflow_run_id: int) -> Any:
    """Query for suspicious TIs.

    Query all task instances that are submitted to distributor or running which haven't
    reported as alive in the allocated time.
    """
    bind_to_logger(workflow_run_id=workflow_run_id)
    logger.info(f"Getting suspicious tis for wfi {workflow_run_id}")
    query = """
        SELECT
            task_instance.id, task_instance.workflow_run_id,
            task_instance.distributor_id, task_instance.cluster_type_id,
            task_instance.array_id, task_instance.array_batch_num,
            task_instance.array_step_id, task_instance.subtask_id
        FROM
            task_instance
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task_instance.status in :active_tasks
            AND task_instance.report_by_date <= CURRENT_TIMESTAMP()
    """
    rows = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(
            active_tasks=[
                TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR,
                TaskInstanceStatus.RUNNING,
            ],
            workflow_run_id=workflow_run_id,
        )
        .all()
    )
    DB.session.commit()
    resp = jsonify(
        task_instances=[ti.to_wire_as_distributor_task_instance() for ti in rows]
    )
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/get_task_instances_to_terminate", methods=["GET"]
)
def get_task_instances_to_terminate(workflow_run_id: int) -> Any:
    """Get the task instances for a given workflow run that need to be terminated."""
    bind_to_logger(workflow_run_id=workflow_run_id)
    logger.info(f"Getting tis that should be terminated for wfr {workflow_run_id}")
    workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()

    if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
        task_instance_states = [TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR]
    if workflow_run.status == WorkflowRunStatus.COLD_RESUME:
        task_instance_states = [
            TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR,
            TaskInstanceStatus.RUNNING,
        ]

    query = """
        SELECT
            task_instance.id, task_instance.workflow_run_id,
            task_instance.distributor_id, task_instance.cluster_type_id,
            task_instance.array_id, task_instance.array_batch_num,
            task_instance.array_step_id, task_instance.subtask_id
        FROM
            task_instance
        WHERE
            task_instance.workflow_run_id = :workflow_run_id
            AND task_instance.status in :task_instance_states
    """
    rows = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(
            task_instance_states=task_instance_states, workflow_run_id=workflow_run_id
        )
        .all()
    )
    DB.session.commit()
    resp = jsonify(
        task_instances=[ti.to_wire_as_distributor_task_instance() for ti in rows]
    )
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<distributor_id>/maxpss/<maxpss>", methods=["POST"]
)
def set_maxpss(distributor_id: int, maxpss: int) -> Any:
    """Route to set maxpss of a job instance.

    Args:
        distributor_id (int): sge distributor id
        maxpss (int): maxpss from QPID
    """
    bind_to_logger(distributor_id=distributor_id)
    logger.info(f"Setting maxpss for distributor_id {distributor_id}")
    try:
        sql = f"UPDATE task_instance SET maxpss={maxpss} WHERE distributor_id={distributor_id}"
        DB.session.execute(sql)
        DB.session.commit()
        resp = jsonify(message=None)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        msg = "Error updating maxpss for distributor id {eid}: {error}".format(
            eid=distributor_id, error=str(e)
        )
        logger.error(msg)
        raise ServerError(
            f"Unexpected Jobmon Server Error {sys.exc_info()[0]} in " f"{request.path}",
            status_code=500,
        ) from e


@finite_state_machine.route(
    "/workflow_run/<workflow_run_id>/log_distributor_report_by", methods=["POST"]
)
def log_distributor_report_by(workflow_run_id: int) -> Any:
    """Log the next report by date and time."""
    data = request.get_json()
    params = {"workflow_run_id": int(workflow_run_id)}
    for key in ["next_report_increment", "distributor_ids"]:
        params[key] = data[key]

    if params["distributor_ids"]:
        query = """
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE
                workflow_run_id = :workflow_run_id
                AND distributor_id in :distributor_ids
        """
        DB.session.execute(query, params)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/task_instance", methods=["POST"])
def add_task_instance() -> Any:
    """Add a task_instance to the database.

    Args:
        task_id (int): unique id for the task
        cluster_type_name (str): string name of the cluster type used
    """
    try:
        data = request.get_json()
        task_id = data["task_id"]
        array_id = data["array_id"]
        array_batch_num = data["array_batch_num"]
        bind_to_logger(task_id=task_id, array_id=array_id)
        logger.info(f"Add task instance for task {task_id}")
        # query task
        task = DB.session.query(Task).filter_by(id=task_id).first()
        DB.session.commit()

        # create task_instance from task parameters
        task_instance = TaskInstance(
            workflow_run_id=data["workflow_run_id"],
            task_id=task_id,
            array_id=array_id,
            array_batch_num=array_batch_num,
            task_resources_id=task.task_resources_id,
        )
        DB.session.add(task_instance)
        DB.session.commit()
        task_instance.task.transition(TaskStatus.INSTANTIATING)
        DB.session.commit()
        resp = jsonify(
            task_instance=task_instance.to_wire_as_distributor_task_instance()
        )
        resp.status_code = StatusCodes.OK
        return resp
    except InvalidStateTransition as e:
        # Handles race condition if the task is already instantiated state
        if task_instance.task.status == TaskStatus.INSTANTIATING:
            msg = (
                "Caught InvalidStateTransition. Not transitioning task "
                "{}'s task_instance_id {} from I to I".format(
                    data["task_id"], task_instance.id
                )
            )
            logger.warning(msg)
            DB.session.commit()
            resp = jsonify(
                task_instance=task_instance.to_wire_as_distributor_task_instance()
            )
            resp.status_code = StatusCodes.OK
            return resp
        else:
            raise ServerError(
                f"Unexpected Jobmon Server Error in {request.path}", status_code=500
            ) from e


@finite_state_machine.route(
    "/get_array_task_instance_id/<array_id>/<batch_num>/<step_id>", methods=['GET']
)
def get_array_task_instance_id(array_id: int, batch_num: int, step_id: int):
    """Given an array ID and an index, select a single task instance ID.

    Task instance IDs that are associated with the array are ordered, and selected by index.
    This route will be called once per array task instance worker node, so must be scalable."""

    bind_to_logger(array_id=array_id)

    query = """
        SELECT id
        FROM task_instance
        WHERE array_id=:array_id
        AND array_batch_num=:batch_num
        AND array_step_id=:step_id"""

    task_instance_id = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(array_id=array_id, batch_num=batch_num, step_id=step_id)
        .one()
    )

    resp = jsonify(task_instance_id=task_instance_id.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_no_distributor_id", methods=["POST"]
)
def log_no_distributor_id(task_instance_id: int) -> Any:
    """Log a task_instance_id that did not get an distributor_id upon submission."""
    bind_to_logger(task_instance_id=task_instance_id)
    logger.info(
        f"Logging ti {task_instance_id} did not get distributor id upon submission"
    )
    data = request.get_json()
    logger.debug(f"Log NO DISTRIBUTOR ID. Data {data['no_id_err_msg']}")
    err_msg = data["no_id_err_msg"]
    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(ti, TaskInstanceStatus.NO_DISTRIBUTOR_ID)
    error = TaskInstanceErrorLog(task_instance_id=ti.id, description=err_msg)
    DB.session.add(error)
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_distributor_id", methods=["POST"]
)
def log_distributor_id(task_instance_id: int) -> Any:
    """Log a task_instance's distributor id.

    Args:
        task_instance_id: id of the task_instance to log
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(
        ti, TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR
    )
    ti.distributor_id = data["distributor_id"]
    if ti.array_id is None:
        ti.subtask_id = str(ti.distributor_id)
    else:
        ti.subtask_id = data["subtask_id"]
    ti.report_by_date = func.ADDTIME(
        func.now(), func.SEC_TO_TIME(data["next_report_increment"])
    )
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_known_error", methods=["POST"]
)
def log_known_error(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance.
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    query = """
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
    """
    ti = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(task_instance_id=task_instance_id)
        .one_or_none()
    )

    try:
        resp = _log_error(ti, error_state, error_message, distributor_id, nodename)
    except sqlalchemy.exc.OperationalError:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ti, error_state, new_msg, distributor_id, nodename)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/log_unknown_error", methods=["POST"]
)
def log_unknown_error(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance
    """
    bind_to_logger(task_instance_id=task_instance_id)
    data = request.get_json()
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    query = """
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
            AND task_instance.report_by_date <= CURRENT_TIMESTAMP()
    """
    ti = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .params(task_instance_id=task_instance_id)
        .one_or_none()
    )

    # make sure the task hasn't logged a new heartbeat since we began
    # reconciliation
    if ti is not None:
        try:
            resp = _log_error(ti, error_state, error_message, distributor_id, nodename)
        except sqlalchemy.exc.OperationalError:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(ti, error_state, new_msg, distributor_id, nodename)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/record_array_batch_num/<batch_num>", methods=["POST"]
)
def record_array_batch_num(batch_num: int) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = request.get_json()
    task_instance_ids_list = data['task_instance_ids']

    task_instance_ids = ",".join(f'{x}' for x in task_instance_ids_list)

    update_stmt = f"""
        UPDATE task_instance
        SET array_batch_num = {batch_num}
        WHERE id IN ({task_instance_ids})
    """
    DB.session.execute(update_stmt)
    DB.session.commit()

    # assign array_step_ids ordered by task_instance_id
    task_instance_ids_list.sort()
    for idx, tid in enumerate(task_instance_ids_list):
        sql = f"""UPDATE task_instance
            SET array_step_id = {idx + 1}
            WHERE id = {tid}"""
        DB.session.execute(sql)
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_instance/<task_instance_id>/set_subtask_id", methods=["POST"]
)
def record_subtask_id(task_instance_id: int) -> Any:
    """Update the actual distributor id of the task/subtask in array.

    Keep this info in DB so we don't need to calculate it from
    array_id, array_batch_num, and array_step_id every time.
    """
    data = request.get_json()
    subtask_id = data['subtask_id']
    sql = f"""
        UPDATE task_instance
        SET subtask_id={subtask_id}
        WHERE id={task_instance_id}"""
    DB.session.execute(sql)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/task_instance/transition/<new_status>", methods=["POST"])
def transition_task_instances(new_status: str) -> Any:
    """Attempt to transition a task instance to the new status"""
    data = request.get_json()
    task_instance_ids = data['task_instance_ids']
    array_id = data.get('array_id', None)
    distributor_id = data['distributor_id']

    if array_id is not None:
        bind_to_logger(array_id=array_id)

    task_instance_id_str = ",".join(f'{x}' for x in task_instance_ids)

    query = f"""
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id IN ({task_instance_id_str})
    """
    task_instances = (
        DB.session.query(TaskInstance)
        .from_statement(text(query))
        .all()
    )

    # Attempt a transition for each task instance
    erroneous_transitions = []
    for ti in task_instances:
        # Attach the distributor ID
        # this will cause problem for non array tasks
        ti.distributor_id = distributor_id
        response = _update_task_instance_state(ti, new_status)
        if len(response) > 0:
            # Task instances that fail to transition log a message, but are returned with
            # their existing state (no exceptions raised).
            erroneous_transitions.append(ti)

    DB.session.flush()
    DB.session.commit()
    resp = jsonify(erroneous_transitions={ti.id: ti.status for ti in erroneous_transitions})
    resp.status_code = StatusCodes.OK
    return resp


# ############################ HELPER FUNCTIONS ###############################
def _update_task_instance_state(task_instance: TaskInstance, status_id: str) -> Any:
    """Advance the states of task_instance and it's associated Task.

    Return any messages that should be published based on the transition.

    Args:
        task_instance (TaskInstance): object of time models.TaskInstance
        status_id (int): id of the status to which to transition
    """
    response = ""
    try:
        task_instance.transition(status_id)
    except InvalidStateTransition:
        if task_instance.status == status_id:
            # It was already in that state, just log it
            msg = (
                f"Attempting to transition to existing state."
                f"Not transitioning task, tid= "
                f"{task_instance.id} from {task_instance.status} to "
                f"{status_id}"
            )
            logger.warning(msg)
            response += msg
        else:
            # Tried to move to an illegal state
            msg = (
                f"Illegal state transition. Not transitioning task, "
                f"tid={task_instance.id}, from {task_instance.status} to "
                f"{status_id}"
            )
            logger.error(msg)
            response += msg
    except KillSelfTransition:
        msg = f"kill self, cannot transition tid={task_instance.id}"
        logger.warning(msg)
        response += msg
    except Exception as e:
        raise ServerError(
            f"General exception in _update_task_instance_state, jid "
            f"{task_instance}, transitioning to {task_instance}. Not "
            f"transitioning task. Server Error in {request.path}",
            status_code=500,
        ) from e
    return response


def _log_error(
    ti: TaskInstance,
    error_state: str,
    error_msg: str,
    distributor_id: Optional[int] = None,
    nodename: Optional[str] = None,
) -> Any:
    if nodename is not None:
        ti.nodename = nodename
    if distributor_id is not None:
        ti.distributor_id = distributor_id

    try:
        error = TaskInstanceErrorLog(task_instance_id=ti.id, description=error_msg)
        DB.session.add(error)
        msg = _update_task_instance_state(ti, error_state)
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        DB.session.rollback()
        raise ServerError(
            f"Unexpected Jobmon Server Error in {request.path}", status_code=500
        ) from e

    return resp
