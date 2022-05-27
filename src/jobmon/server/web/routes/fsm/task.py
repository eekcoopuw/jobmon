"""Routes for Tasks."""
from http import HTTPStatus as StatusCodes
import json
from typing import Any, cast, Dict, List, Set, Union

from flask import jsonify, request
from sqlalchemy import desc, insert, select, tuple_
from sqlalchemy.orm import Session
from sqlalchemy.exc import DataError
import structlog

from jobmon import constants
from jobmon.server.web._compat import add_ignore
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


logger = structlog.get_logger(__name__)


@blueprint.route("/task/bind_tasks", methods=["PUT"])
def bind_tasks() -> Any:
    """Bind the task objects to the database."""
    all_data = cast(Dict, request.get_json())
    tasks = all_data["tasks"]
    workflow_id = int(all_data["workflow_id"])
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    logger.info("Binding tasks")
    # receive from client the tasks in a format of:
    # {<hash>:[node_id(1), task_args_hash(2), array_id(3), task_resources_id(4), name(5),
    # command(6), max_attempts(7), reset_if_running(8),
    # task_args(9),task_attributes(10),resource_scales(11)]}

    session = SessionLocal()
    with session.begin():
        # Retrieve existing task_ids
        task_select_stmt = select(
            Task
        ).where(
            (Task.workflow_id == workflow_id),
            tuple_(
                Task.node_id, Task.task_args_hash
            ).in_(
                [tuple_(task[0], task[1]) for task in tasks.values()]
            )
        )
        prebound_tasks = session.execute(task_select_stmt).scalars().all()

        # Bind tasks not present in DB
        tasks_to_add: List[Dict] = []  # Container for tasks not yet bound to the database
        present_tasks = {
            (task.node_id, task.task_args_hash): task for task in prebound_tasks
        }  # Dictionary mapping existing Tasks to the supplied arguments
        arg_attr_mapping = {}  # Dict mapping input tasks to the corresponding args/attributes
        task_hash_lookup = {}  # Reverse dictionary of inputs, maps hash back to values
        for hashval, items in tasks.items():

            (
                node_id,
                arg_hash,
                array_id,
                task_resources_id,
                name,
                command,
                max_att,
                reset,
                args,
                attrs,
                resource_scales,
                fallback_queues,
            ) = items

            id_tuple = (node_id, arg_hash)

            # Conditional logic: Has task already been bound to the DB? If yes, reset the
            # task status and update the args/attributes
            if id_tuple in present_tasks.keys():
                task = present_tasks[id_tuple]
                task.reset(
                    name=name, command=command, max_attempts=max_att, reset_if_running=reset
                )

            # If not, add the task
            else:
                task = {
                    "workflow_id": workflow_id,
                    "node_id": node_id,
                    "task_args_hash": arg_hash,
                    "array_id": array_id,
                    "task_resources_id": task_resources_id,
                    "name": name,
                    "command": command,
                    "max_attempts": max_att,
                    "status": constants.TaskStatus.REGISTERING,
                    "resource_scales": str(resource_scales),
                    "fallback_queues": str(fallback_queues),
                }
                tasks_to_add.append(task)

            arg_attr_mapping[hashval] = (args, attrs)
            task_hash_lookup[id_tuple] = hashval

        # Update existing tasks
        if present_tasks:

            # ORM task objects already updated in task.reset, flush the changes
            session.flush()

        # Bind new tasks with raw SQL
        if len(tasks_to_add):
            # This command is guaranteed to succeed, since names are truncated in the client
            task_insert_stmt = insert(Task).values(tasks_to_add)
            session.execute(task_insert_stmt)
            session.flush()

            # Fetch newly bound task ids
            new_task_query = select(
                Task
            ).where(
                (Task.workflow_id == workflow_id),
                tuple_(
                    Task.node_id, Task.task_args_hash
                ).in_(
                    [tuple_(task['node_id'], task['task_args_hash']) for task in tasks_to_add]
                )
            )
            new_tasks = session.execute(new_task_query).scalars().all()

        else:
            # Empty task list
            new_tasks = []

        # Create the response dict of tasks {<hash>: [id, status]}
        # Done here to prevent modifying tasks, and necessitating a refresh.
        return_tasks = {}

        for task in prebound_tasks + new_tasks:
            id_tuple = (task.node_id, task.task_args_hash)
            hashval = task_hash_lookup[id_tuple]
            return_tasks[hashval] = [task.id, task.status]

        # Add new task attribute types
        attr_names = set([name for x in arg_attr_mapping.values() for name in x[1]])
        if attr_names:
            task_attributes_types = _add_or_get_attribute_type(attr_names, session)

            # Map name to ID from resultant list
            task_attr_type_mapping = {ta.name: ta.id for ta in task_attributes_types}
        else:
            task_attr_type_mapping = {}

        # Add task_args and attributes to the DB

        args_to_add = []
        attrs_to_add = []

        for hashval, task in return_tasks.items():

            task_id = task[0]
            args, attrs = arg_attr_mapping[hashval]

            for key, val in args.items():
                task_arg = {"task_id": task_id, "arg_id": key, "val": val}
                args_to_add.append(task_arg)

            for name, val in attrs.items():
                # An interesting bug: the attribute type names are inserted using the
                # insert.prefix("IGNORE") syntax, which silently truncates names that are
                # overly long. So this will raise a keyerror if the attribute name is >255
                # characters. Don't imagine this is a serious issue but might be worth protecting
                attr_type_id = task_attr_type_mapping[name]
                insert_vals = {
                    "task_id": task_id,
                    "task_attribute_type_id": attr_type_id,
                    "value": val,
                }
                attrs_to_add.append(insert_vals)

        if args_to_add:
            try:
                arg_insert_stmt = (
                    insert(TaskArg)
                    .values(args_to_add)
                )
                if SessionLocal.bind.dialect.name == "mysql":
                    arg_insert_stmt = arg_insert_stmt.on_duplicate_key_update(
                        val=arg_insert_stmt.inserted.val
                    )
                elif SessionLocal.bind.dialect.name == "sqlite":
                    pass
                else:
                    raise ServerError(
                        "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
                        + SessionLocal.bind.dialect.name
                    )
                session.execute(arg_insert_stmt)
            except DataError as e:
                # Args likely too long, message back
                raise InvalidUsage(
                    "Task Args are constrained to 1000 characters, you may have values "
                    f"that are too long. Message: {str(e)}",
                    status_code=400,
                ) from e

        if attrs_to_add:
            try:
                attr_insert_stmt = (
                    insert(TaskAttribute)
                    .values(attrs_to_add)
                )
                if SessionLocal.bind.dialect.name == "mysql":
                    attr_insert_stmt = attr_insert_stmt.on_duplicate_key_update(
                        val=attr_insert_stmt.inserted.val
                    )
                elif SessionLocal.bind.dialect.name == "sqlite":
                    pass
                else:
                    raise ServerError(
                        "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
                        + SessionLocal.bind.dialect.name
                    )
                session.execute(attr_insert_stmt)

            except DataError as e:
                # Attributes too long, message back
                raise InvalidUsage(
                    "Task attributes are constrained to 255 characters, you may have values "
                    f"that are too long. Message: {str(e)}",
                    status_code=400,
                ) from e

    resp = jsonify(tasks=return_tasks)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_attribute_type(
    names: Union[List[str], Set[str]],
    session: Session
) -> List[TaskAttributeType]:
    attribute_types = [{"name": name} for name in names]
    try:
        with session.begin_nested():
            insert_stmt = add_ignore(insert(TaskAttributeType))
            session.execute(insert_stmt, attribute_types)
    except DataError as e:
        raise InvalidUsage(
            "Attribute types are constrained to 255 characters, your "
            f"attributes might be too long. Message: {str(e)}",
            status_code=400,
        ) from e

    # Query the IDs
    select_stmt = select(
        TaskAttributeType
    ).where(
        TaskAttributeType.name.in_(names)
    )
    attribute_type_ids = session.execute(select_stmt).scalars().all()
    return attribute_type_ids


@blueprint.route("/task/bind_resources", methods=["POST"])
def bind_task_resources() -> Any:
    """Add the task resources for a given task."""
    data = cast(Dict, request.get_json())

    session = SessionLocal()
    with session.begin():

        new_resources = TaskResources(
            queue_id=data["queue_id"],
            task_resources_type_id=data.get("task_resources_type_id", None),
            requested_resources=json.dumps(data.get("requested_resources", None)),
        )
        session.add(new_resources)

    resp = jsonify(new_resources.id)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task/<task_id>/most_recent_ti_error", methods=["GET"])
def get_most_recent_ti_error(task_id: int) -> Any:
    """Route to determine the cause of the most recent task_instance's error.

    Args:
        task_id (int): the ID of the task.

    Return:
        error message
    """
    structlog.threadlocal.bind_threadlocal(task_id=task_id)
    logger.info(f"Getting most recent ji error for ti {task_id}")

    session = SessionLocal()
    with session.begin():

        select_stmt = select(
            TaskInstanceErrorLog
        ).join_from(
            TaskInstance, TaskInstanceErrorLog,
            TaskInstance.id == TaskInstanceErrorLog.task_instance_id
        ).where(
            TaskInstance.task_id == task_id
        ).order_by(
            desc(TaskInstance.id)
        ).limit(1)
        ti_error = session.execute(select_stmt).scalars().one_or_none()

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
