"""Routes for TaskInstances."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, Optional

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
import structlog

from jobmon import constants
from jobmon.exceptions import InvalidStateTransition
from jobmon.serializers import SerializeTaskInstanceBatch
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import ServerError


logger = structlog.get_logger(__name__)


@blueprint.route("/task_instance/<task_instance_id>/log_running", methods=["POST"])
def log_running(task_instance_id: int) -> Any:
    """Log a task_instance as running.

    Args:
        task_instance_id: id of the task_instance to log as running
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())

    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        if data.get("distributor_id", None) is not None:
            task_instance.distributor_id = data["distributor_id"]
        if data.get("nodename", None) is not None:
            task_instance.nodename = data["nodename"]
        task_instance.process_group_id = data["process_group_id"]
        try:
            task_instance.transition(constants.TaskInstanceStatus.RUNNING)
            task_instance.report_by_date = func.ADDTIME(
                func.now(), func.SEC_TO_TIME(data["next_report_increment"])
            )
        except InvalidStateTransition as e:
            if task_instance.status == constants.TaskInstanceStatus.RUNNING:
                logger.warning(e)
            elif task_instance.status == constants.TaskInstanceStatus.KILL_SELF:
                task_instance.transition(constants.TaskInstanceStatus.ERROR_FATAL)
            else:
                # Tried to move to an illegal state
                logger.error(e)

        session.commit()

    resp = jsonify(task_instance=task_instance.to_wire_as_worker_node_task_instance())
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_report_by", methods=["POST"])
def log_ti_report_by(task_instance_id: int) -> Any:
    """Log a task_instance as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())

    with SessionLocal.begin() as session:
        next_report_increment = data["next_report_increment"]
        update_stmt = update(
            TaskInstance
        ).where(
            TaskInstance.id == task_instance_id
        )
        vals = {
            "report_by_date": func.ADDTIME(func.CURRENT_TIMESTAMP(), next_report_increment)
        }
        distributor_id = data.get("distributor_id", None)
        if distributor_id is not None:
            vals["distributor_id"] = distributor_id
        session.execute(update_stmt.values(**vals))
        session.commit()

        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()
        if task_instance.status == constants.TaskInstanceStatus.TRIAGING:
            task_instance.transition(constants.TaskInstanceStatus.RUNNING)
        session.commit()

    resp = jsonify(status=task_instance.status)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/log_report_by/batch", methods=["POST"])
def log_ti_report_by_batch() -> Any:
    """Log task_instances as being responsive with a new report_by_date.

    This is done at the worker node heartbeat_interval rate, so it may not happen at the same
    rate that the reconciler updates batch submitted report_by_dates (also because it causes
    a lot of traffic if all workers are logging report by_dates often compared to if the
    reconciler runs often).

    Args:
        task_instance_id: id of the task_instance to log
    """
    data = cast(Dict, request.get_json())
    tis = data.get("task_instance_ids", None)

    next_report_increment = data.get("next_report_increment")

    logger.debug(f"Log report_by for TI {tis}.")
    if tis:
        with SessionLocal.begin() as session:
            update_stmt = update(
                TaskInstance
            ).where(
                TaskInstance.id.in_(tis),
                TaskInstance.status == constants.TaskInstanceStatus.LAUNCHED
            ).values(
                func.ADDTIME(func.CURRENT_TIMESTAMP(), next_report_increment)
            )

            session.execute(update_stmt)
            session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_done", methods=["POST"])
def log_done(task_instance_id: int) -> Any:
    """Log a task_instance as done.

    Args:
        task_instance_id: id of the task_instance to log done
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())

    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        if data.get("distributor_id", None) is not None:
            task_instance.distributor_id = data["distributor_id"]
        if data.get("nodename", None) is not None:
            task_instance.nodename = data["nodename"]
        try:
            task_instance.transition(constants.TaskInstanceStatus.DONE)
        except InvalidStateTransition as e:
            if task_instance.status == constants.TaskInstanceStatus.DONE:
                logger.warning(e)
            else:
                # Tried to move to an illegal state
                logger.error(e)
        session.commit()

    resp = jsonify(status=task_instance.status)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_error_worker_node", methods=["POST"])
def log_error_worker_node(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (str): id of the task_instance to log done
        error_message (str): message to log as error
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())
    error_state = data["error_state"]
    error_msg = data["error_message"].encode("latin1", "replace").decode("utf-8")
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        if nodename is not None:
            task_instance.nodename = nodename
        if distributor_id is not None:
            task_instance.distributor_id = distributor_id
        try:
            task_instance.transition(error_state)
            error = TaskInstanceErrorLog(
                task_instance_id=task_instance.id, description=error_msg
            )
            session.add(error)
        except InvalidStateTransition as e:
            if task_instance.status == error_state:
                logger.warning(e)
            else:
                # Tried to move to an illegal state
                logger.error(e)
        session.commit()

    resp = jsonify(status=task_instance.status)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/task_instance_error_log", methods=["GET"])
def get_task_instance_error_log(task_instance_id: int) -> Any:
    """Route to return all task_instance_error_log entries of the task_instance_id.

    Args:
        task_instance_id (int): ID of the task instance

    Return:
        jsonified task_instance_error_log result set
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    logger.info(f"Getting task instance error log for ti {task_instance_id}")

    with SessionLocal.begin() as session:
        select_stmt = select(
            TaskInstanceErrorLog
        ).where(
            TaskInstanceErrorLog.task_instance_id == task_instance_id
        ).order_by(
            TaskInstanceErrorLog.task_instance_id
        )
        res = session.execute(select_stmt).scalars().all()
        session.commit()

    resp = jsonify(task_instance_error_log=[tiel.to_wire() for tiel in res])
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/get_array_task_instance_id/<array_id>/<batch_num>/<step_id>", methods=["GET"]
)
def get_array_task_instance_id(array_id: int, batch_num: int, step_id: int) -> Any:
    """Given an array ID and an index, select a single task instance ID.

    Task instance IDs that are associated with the array are ordered, and selected by index.
    This route will be called once per array task instance worker node, so must be scalable.
    """
    structlog.threadlocal.bind_threadlocal(array_id=array_id)

    with SessionLocal.begin() as session:
        select_stmt = select(
            TaskInstance
        ).where(
            TaskInstance.array_id == array_id,
            TaskInstance.array_batch_num == batch_num,
            TaskInstance.array_step_id == step_id
        )
        task_instance = session.execute(select_stmt).scalars().one()

    resp = jsonify(task_instance_id=task_instance.id)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_no_distributor_id", methods=["POST"])
def log_no_distributor_id(task_instance_id: int) -> Any:
    """Log a task_instance_id that did not get an distributor_id upon submission."""
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    logger.info(f"Logging ti {task_instance_id} did not get distributor id upon submission")
    data = cast(Dict, request.get_json())
    logger.debug(f"Log NO DISTRIBUTOR ID. Data {data['no_id_err_msg']}")
    err_msg = data["no_id_err_msg"]

    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()
        msg = _update_task_instance_state(
            task_instance, constants.TaskInstanceStatus.NO_DISTRIBUTOR_ID
        )
        error = TaskInstanceErrorLog(task_instance_id=task_instance.id, description=err_msg)
        session.add(error)
        session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_distributor_id", methods=["POST"])
def log_distributor_id(task_instance_id: int) -> Any:
    """Log a task_instance's distributor id.

    Args:
        task_instance_id: id of the task_instance to log
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())
    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()
        msg = _update_task_instance_state(task_instance, constants.TaskInstanceStatus.LAUNCHED)
        task_instance.distributor_id = data["distributor_id"]
        task_instance.report_by_date = func.ADDTIME(
            func.now(), func.SEC_TO_TIME(data["next_report_increment"])
        )
        session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_known_error", methods=["POST"])
def log_known_error(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance.
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    with SessionLocal.begin() as session:
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        try:
            resp = _log_error(
                session, task_instance, error_state, error_message, distributor_id, nodename
            )
        except sqlalchemy.exc.OperationalError:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(
                session, task_instance, error_state, new_msg, distributor_id, nodename
            )

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/<task_instance_id>/log_unknown_error", methods=["POST"])
def log_unknown_error(task_instance_id: int) -> Any:
    """Log a task_instance as errored.

    Args:
        task_instance_id (int): id for task instance
    """
    structlog.threadlocal.bind_threadlocal(task_instance_id=task_instance_id)
    data = cast(Dict, request.get_json())
    error_state = data["error_state"]
    error_message = data["error_message"]
    distributor_id = data.get("distributor_id", None)
    nodename = data.get("nodename", None)
    logger.info(f"Log ERROR for TI:{task_instance_id}.")

    with SessionLocal.begin() as session:
        # make sure the task hasn't logged a new heartbeat since we began
        # reconciliation
        select_stmt = select(
            TaskInstance
        ).where(
            TaskInstance.id == task_instance_id,
            TaskInstance.report_by_date <= func.CURRENT_TIMESTAMP()
        )
        task_instance = session.execute(select_stmt).scalars().one_or_none()

    if task_instance is not None:
        try:
            resp = _log_error(
                session, task_instance, error_state, error_message, distributor_id, nodename
            )
        except sqlalchemy.exc.OperationalError:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(
                session, task_instance, error_state, new_msg, distributor_id, nodename
            )

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_instance/instantiate_task_instances", methods=["POST"])
def instantiate_task_instances() -> Any:
    """Sync status of given task intance IDs."""
    data = cast(Dict, request.get_json())
    task_instance_ids_list = tuple([int(tid) for tid in data["task_instance_ids"]])

    with SessionLocal.begin() as session:
        try:

            # update the task table where FSM allows it
            task_update = (
                update(Task)
                .where(
                    Task.id.in_(
                        select(Task.id)
                        .join(TaskInstance, TaskInstance.task_id == Task.id)
                        .where(
                            TaskInstance.id.in_(task_instance_ids_list),
                            (Task.status == constants.TaskStatus.QUEUED),
                        )
                    )
                )
                .values(status=constants.TaskStatus.INSTANTIATING, status_date=func.now())
                .execution_options(synchronize_session=False)
            )
            session.execute(task_update)

            # then propagate back into task instance where a change was made
            task_instance_update = (
                update(TaskInstance)
                .where(
                    TaskInstance.id.in_(
                        select(TaskInstance.id)
                        .join(Task, TaskInstance.task_id == Task.id)
                        .where(
                            # a successful transition
                            (Task.status == constants.TaskStatus.INSTANTIATING),
                            # and part of the current set
                            TaskInstance.id.in_(task_instance_ids_list),
                        )
                    )
                )
                .values(
                    status=constants.TaskInstanceStatus.INSTANTIATED,
                    status_date=func.now())
                .execution_options(synchronize_session=False)
            )
            session.execute(task_instance_update)

        except Exception:
            session.rollback()
            raise
        else:
            session.commit()

            instantiated_batches_query = (
                select(
                    TaskInstance.array_id,
                    TaskInstance.array_batch_num,
                    TaskInstance.task_resources_id,
                    func.group_concat(TaskInstance.id),
                )
                .where(
                    TaskInstance.id.in_(task_instance_ids_list)
                    & (TaskInstance.status == constants.TaskInstanceStatus.INSTANTIATED)
                )
                .group_by(
                    TaskInstance.array_id,
                    TaskInstance.array_batch_num,
                    TaskInstance.task_resources_id,
                )
            )
            result = session.execute(instantiated_batches_query)
            serialized_batches = []
            for array_id, array_batch_num, task_resources_id, task_instance_ids in result:
                task_instance_ids = [
                    int(task_instance_id)
                    for task_instance_id in task_instance_ids.split(",")
                ]
                array_name_query = select(Array.name).where(Array.id == array_id)
                result = session.execute(array_name_query).fetchone()
                array_name = result["name"]
                serialized_batches.append(
                    SerializeTaskInstanceBatch.to_wire(
                        array_id=array_id,
                        array_name=array_name,
                        array_batch_num=array_batch_num,
                        task_resources_id=task_resources_id,
                        task_instance_ids=task_instance_ids,
                    )
                )

    resp = jsonify(task_instance_batches=serialized_batches)
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
    except Exception as e:
        raise ServerError(
            f"General exception in _update_task_instance_state, jid "
            f"{task_instance}, transitioning to {task_instance}. Not "
            f"transitioning task. Server Error in {request.path}",
            status_code=500,
        ) from e
    return response


def _log_error(
    session: Session,
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
        session.add(error)
        msg = _update_task_instance_state(ti, error_state)
        session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        session.rollback()
        raise ServerError(
            f"Unexpected Jobmon Server Error in {request.path}", status_code=500
        ) from e

    return resp
