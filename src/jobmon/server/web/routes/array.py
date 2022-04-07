"""Routes for Arrays."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
from sqlalchemy import bindparam, func, insert, literal_column, select, text, update
from werkzeug.local import LocalProxy

from jobmon.constants import TaskInstanceStatus
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
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


@finite_state_machine.route("/array/<array_id>/queue_task_batch", methods=["POST"])
def record_array_batch_num(array_id: int) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = request.get_json()
    array_id = int(array_id)
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])

    try:
        # update task status to acquire lock
        update_stmt = update(
            Task
        ).where(
            Task.id.in_(task_ids)
            & Task.status.in_([TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES])
        ).values(
            status=TaskStatus.QUEUED,
            status_date=func.now(),
            num_attempts=(Task.num_attempts + 1)
        )
        DB.session.execute(update_stmt)

        # now insert them into task instance
        insert_stmt = insert(TaskInstance).from_select(
            # columns map 1:1 to selected rows
            ["task_id", "workflow_run_id", "array_id", "task_resources_id", "array_batch_num",
             "array_step_id", "status", "status_date"],

            # select statement
            select(
                # unique id
                Task.id.label("task_id"),

                # static associations
                literal_column(str(workflow_run_id)).label("workflow_run_id"),
                literal_column(str(array_id)).label("array_id"),
                literal_column(str(task_resources_id)).label("task_resources_id"),

                # batch info
                select(
                    func.coalesce(func.max(TaskInstance.array_batch_num) + 1, 1)
                ).where(
                    (TaskInstance.workflow_run_id == workflow_run_id)
                    & (TaskInstance.array_id == array_id)
                ).label("array_batch_num"),
                (func.row_number().over(order_by=Task.id) - 1).label("array_step_id"),

                # status columns
                literal_column(f"'{TaskInstanceStatus.QUEUED}'").label("status"),
                func.now().label("status_date")

            ).where(
                Task.id.in_(task_ids) & (Task.status == TaskStatus.QUEUED)
            ),

            # no python side defaults. Server defaults only
            include_defaults=False
        )
        DB.session.execute(insert_stmt)
    except Exception:
        DB.session.rollback()
        raise
    else:
        DB.session.commit()

        tasks_by_status_query = select(
            Task.status, func.group_concat(Task.id)
        ).where(
            Task.id.in_(task_ids)
        ).group_by(Task.status)
        result_dict = {}
        for row in DB.session.execute(tasks_by_status_query):
            result_dict[row[0]] = [int(i) for i in row[1].split(",")]

    resp = jsonify(tasks_by_status=result_dict)
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
    try:
        # Acquire a lock and update tasks to launched
        update_task_stmt = (
            update(Task).
            where(
                Task.id in (
                    select(
                        TaskInstance.task_id
                    ).where(
                        TaskInstance.array_id == array_id,
                        TaskInstance.array_batch_num == batch_num
                    )
                ),
                Task.status == TaskStatus.INSTANTIATING
            ).
            values(
                status=TaskStatus.LAUNCHED,
                status_date=func.now()
            )
        )
        DB.session.execute(update_task_stmt)

        # Transition all the task instances in the batch
        # Bypassing the ORM for performance reasons.
        update_stmt = (
            update(TaskInstance).
            where(TaskInstance.array_id == array_id,
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
    except Exception:
        DB.session.rollback()
        raise
