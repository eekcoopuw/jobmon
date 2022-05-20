"""Routes for Arrays."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from flask import jsonify, request
from sqlalchemy import bindparam, func, insert, literal_column, select, update
import structlog

from jobmon.constants import TaskInstanceStatus
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint


logger = structlog.get_logger(__name__)


@blueprint.route("/array", methods=["POST"])
def add_array() -> Any:
    """Return an array ID by workflow and task template version ID.

    If not found, bind the array.
    """
    data = cast(Dict, request.get_json())
    workflow_id = int(data["workflow_id"])
    task_template_version_id = int(data["task_template_version_id"])

    structlog.threadlocal.bind_threadlocal(
        task_template_version_id=task_template_version_id,
        workflow_id=workflow_id,
    )

    # Check if the array is already bound, if so return it
    with SessionLocal.begin() as session:
        select_stmt = select(
            Array
        ).where(
            workflow_id=workflow_id,
            task_template_version_id=task_template_version_id
        )
        array = session.execute(select_stmt).scalars().one_or_none()

        if array is None:  # not found, so need to add it
            array = Array(
                task_template_version_id=data["task_template_version_id"],
                workflow_id=data["workflow_id"],
                max_concurrently_running=data["max_concurrently_running"],
                name=data["name"],
            )
            session.add(array)
        else:
            update_stmt = update(
                Array
            ).where(
                Array.id == array.id
            ).values(
                max_concurrently_running=data["max_concurrently_running"]
            )
            session.execute(update_stmt)
        session.commit()

    # return result
    resp = jsonify(array_id=array.id)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/array/<array_id>/queue_task_batch", methods=["POST"])
def record_array_batch_num(array_id: int) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, request.get_json())
    array_id = int(array_id)
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])

    with SessionLocal.begin() as session:

        try:
            # update task status to acquire lock
            update_stmt = (
                update(Task)
                .where(
                    Task.id.in_(task_ids),
                    Task.status.in_(
                        [TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]
                    ),
                )
                .values(
                    status=TaskStatus.QUEUED,
                    status_date=func.now(),
                    num_attempts=(Task.num_attempts + 1),
                )
            )
            session.execute(update_stmt)

            # now insert them into task instance
            insert_stmt = insert(TaskInstance).from_select(
                # columns map 1:1 to selected rows
                [
                    "task_id",
                    "workflow_run_id",
                    "array_id",
                    "task_resources_id",
                    "array_batch_num",
                    "array_step_id",
                    "status",
                    "status_date",
                ],
                # select statement
                select(
                    # unique id
                    Task.id.label("task_id"),
                    # static associations
                    literal_column(str(workflow_run_id)).label("workflow_run_id"),
                    literal_column(str(array_id)).label("array_id"),
                    literal_column(str(task_resources_id)).label("task_resources_id"),
                    # batch info
                    select(func.coalesce(func.max(TaskInstance.array_batch_num) + 1, 1))
                    .where(
                        (TaskInstance.workflow_run_id == workflow_run_id)
                        & (TaskInstance.array_id == array_id)
                    )
                    .label("array_batch_num"),
                    (func.row_number().over(order_by=Task.id) - 1).label("array_step_id"),
                    # status columns
                    literal_column(f"'{TaskInstanceStatus.QUEUED}'").label("status"),
                    func.now().label("status_date"),
                ).where(Task.id.in_(task_ids) & (Task.status == TaskStatus.QUEUED)),
                # no python side defaults. Server defaults only
                include_defaults=False,
            )
            session.execute(insert_stmt)
            session.commit()
        except Exception:
            session.rollback()
            raise
        else:
            tasks_by_status_query = (
                select(Task.status, func.group_concat(Task.id))
                .where(Task.id.in_(task_ids))
                .group_by(Task.status)
            )
            result_dict = {}
            for row in session.execute(tasks_by_status_query):
                result_dict[row[0]] = [int(i) for i in row[1].split(",")]
            session.commit()

    resp = jsonify(tasks_by_status=result_dict)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/array/<array_id>/transition_to_launched", methods=["POST"])
def transition_array_to_launched(array_id: int) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    structlog.threadlocal.bind_threadlocal(array_id=array_id)

    data = cast(Dict, request.get_json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    with SessionLocal.begin() as session:
        try:
            # Acquire a lock and update tasks to launched
            update_task_stmt = (
                update(Task)
                .where(
                    Task.id.in_(
                        select(TaskInstance.task_id).where(
                            TaskInstance.array_id == array_id,
                            TaskInstance.array_batch_num == batch_num,
                        )
                    ),
                    Task.status == TaskStatus.INSTANTIATING,
                )
                .values(status=TaskStatus.LAUNCHED, status_date=func.now())
            ).execution_options(synchronize_session=False)
            session.execute(update_task_stmt)

            # Transition all the task instances in the batch
            # Bypassing the ORM for performance reasons.
            update_stmt = (
                update(TaskInstance)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.status == TaskInstanceStatus.INSTANTIATED,
                    TaskInstance.array_batch_num == batch_num,
                )
                .values(
                    status=TaskInstanceStatus.LAUNCHED,
                    submitted_date=func.now(),
                    status_date=func.now(),
                    report_by_date=func.ADDTIME(func.now(), func.SEC_TO_TIME(next_report)),
                )
            ).execution_options(synchronize_session=False)

            session.execute(update_stmt)
            session.commit()
        except Exception:
            session.rollback()
            raise

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/array/<array_id>/log_distributor_id", methods=["POST"])
def log_array_distributor_id(array_id: int) -> Any:
    """Add distributor_id, stderr/stdout paths to the DB for all TIs in an array."""
    data = cast(Dict, request.get_json())
    batch_num = data["array_batch_num"]
    distributor_id_map = data["distributor_id_map"]

    # Create a list of dicts out of the distributor id map.
    params = [
        {"step_id": key, "distributor_id": val[0], "stdout": val[1], "stderr": val[2]}
        for key, val in distributor_id_map.items()
    ]

    with SessionLocal.begin() as session:

        # Acquire a lock and update the task instance table
        # Using bindparam only issues one query; unfortunately, the MariaDB optimizer actually
        # performs this operation iteratively. The update is fairly slow despite the fact that
        # we are issuing a single bulk query.
        try:
            update_stmt = (
                update(TaskInstance)
                .where(
                    TaskInstance.array_batch_num == batch_num,
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_step_id == bindparam("step_id"),
                )
                .values(distributor_id=bindparam("distributor_id"),
                        stdout=bindparam("stdout"),
                        stderr=bindparam("stderr"))
                .execution_options(synchronize_session=False)
            )
            session.execute(update_stmt, params)

            # Return the affected rows and their distributor ids
            select_stmt = (
                select(TaskInstance.id, TaskInstance.distributor_id)
                .where(
                    TaskInstance.array_batch_num == batch_num,
                    TaskInstance.array_id == array_id,
                )
                .execution_options(synchronize_session=False)
            )

            res = session.execute(select_stmt).fetchall()
            session.commit()

        except Exception:
            session.rollback()
            raise

    resp = jsonify(task_instance_map={ti.id: ti.distributor_id for ti in res})
    resp.status_code = StatusCodes.OK
    return resp
