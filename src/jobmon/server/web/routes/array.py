"""Routes for Arrays."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy import bindparam, select, text, update
from sqlalchemy.sql import func
from werkzeug.local import LocalProxy

from jobmon.constants import TaskInstanceStatus
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.routes import finite_state_machine


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


@finite_state_machine.route("/array", methods=["POST"])
def add_array() -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = request.get_json()
    bind_to_logger(
        task_template_version_id=data["task_template_version_id"],
        workflow_id=data["workflow_id"],
    )

    # Check if the array is already bound, if so return it
    array_stmt = """
        SELECT array.*
        FROM array
        WHERE
            workflow_id = :workflow_id
        AND
            task_template_version_id = :task_template_version_id"""
    array = (
        DB.session.query(Array)
        .from_statement(text(array_stmt))
        .params(
            workflow_id=data["workflow_id"],
            task_template_version_id=data["task_template_version_id"],
        )
        .one_or_none()
    )
    DB.session.commit()

    if array is None:  # not found, so need to add it
        array = Array(
            task_template_version_id=data["task_template_version_id"],
            workflow_id=data["workflow_id"],
            max_concurrently_running=data["max_concurrently_running"],
        )
        DB.session.add(array)
    else:
        array.max_concurrently_running = data["max_concurrently_running"]
    DB.session.commit()

    # return result
    resp = jsonify(array_id=array.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/array/<array_id>", methods=["GET"])
def get_array(array_id: int) -> Any:
    """Return an array.

    If not found, bind the array.
    """
    bind_to_logger(array_id=array_id)

    # Check if the array is already bound, if so return it
    array_stmt = """
        SELECT array.*
        FROM array
        WHERE
            array.id = :array_id
    """
    array = (
        DB.session.query(Array)
        .from_statement(text(array_stmt))
        .params(array_id=array_id)
        .one()
    )
    DB.session.commit()

    resp = jsonify(array=array.to_wire_as_distributor_array())
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/array/<array_id>/get_last_batch_number", methods=["GET"])
def get_last_batch_number(array_id: int) -> int:
    """Route to determine the last_batch_number = max(array_batch_num) from
    the array's task_instances.

    Args:
        array_id (int): the ID of the array.

    Return:
        last_batch_number: int
    """
    bind_to_logger(array_id=array_id)

    sql = f"""
        SELECT array_id, max(array_batch_num) AS last_batch_number
        FROM task_instance
        WHERE
            array_id = {array_id}
        GROUP BY array_id
    """

    row = DB.session.execute(sql).fetchone()

    resp = jsonify(last_batch_number=row[1])
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/array/<array_id>/log_distributor_id", methods=['POST'])
def log_array_distributor_id(array_id: int):

    bind_to_logger(array_id=array_id)

    data = request.get_json()
    batch_num = data["batch_number"]
    distributor_id_map = data["distributor_id_map"]
    next_report = data['next_report_increment']

    # Create a list of dicts out of the distributor id map.
    params = [{'step_id': key, 'distributor_id': val}
              for key, val in distributor_id_map.items()]

    # Transition all the task instances in the batch
    # Bypassing the ORM for performance reasons.

    # TODO: Audit jobmon for any other race conditions.
    #  Specifically ensure the worker node and the client cannot
    #  transition task instances out of instantiated.
    update_stmt = (
        update(TaskInstance).
        where(TaskInstance.array_id == array_id,
              # Is the status WHERE statement necessary? Should always be in this state
              # based on the point this method is called in the distributor service.
              # If it isn't in instantiated, we probably have issues
              TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
              TaskInstance.array_batch_num == batch_num,
              TaskInstance.array_step_id == bindparam('step_id')
              ).
        values(distributor_id=bindparam('distributor_id'),
               status=TaskInstanceStatus.LAUNCHED,
               status_date=func.now(),
               report_by_date=func.ADDTIME(func.now(), func.SEC_TO_TIME(next_report)))
    )

    DB.session.execute(update_stmt, params)
    DB.session.commit()

    # Return the affected rows and their distributor ids
    select_stmt = (
        select(TaskInstance.id, TaskInstance.distributor_id).
        where(TaskInstance.status == TaskInstanceStatus.LAUNCHED,
              TaskInstance.array_batch_num == batch_num,
              TaskInstance.array_id == array_id)
    )

    res = DB.session.execute(select_stmt).fetchall()
    DB.session.commit()

    resp = jsonify(task_instance_map={ti.id: ti.distributor_id for ti in res})
    resp.status_code = StatusCodes.OK
    return resp
