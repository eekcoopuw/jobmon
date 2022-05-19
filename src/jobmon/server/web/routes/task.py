"""Routes for Tasks."""
from http import HTTPStatus as StatusCodes
import json
from typing import Any, cast, Dict, List, Set, Union

from flask import jsonify, request
from sqlalchemy import insert, select, tuple_
from sqlalchemy.orm import Session
from sqlalchemy.exc import DataError
import structlog

from jobmon import constants
from jobmon.serializers import SerializeTaskResourceUsage
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.routes import finite_state_machine, SessionLocal
from jobmon.server.web.routes._compat import add_ignore
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


_task_instance_label_mapping = {
    "Q": "PENDING",
    "B": "PENDING",
    "I": "PENDING",
    "R": "RUNNING",
    "E": "FATAL",
    "Z": "FATAL",
    "W": "FATAL",
    "U": "FATAL",
    "K": "FATAL",
    "D": "DONE",
}

_reversed_task_instance_label_mapping = {
    "PENDING": ["Q", "B", "I"],
    "RUNNING": ["R"],
    "FATAL": ["E", "Z", "W", "U", "K"],
    "DONE": ["D"],
}


@finite_state_machine.route("/task/bind_tasks", methods=["PUT"])
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

    with SessionLocal.begin() as session:
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
            (task.node_id, int(task.task_args_hash)): task for task in prebound_tasks
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

            id_tuple = (node_id, int(arg_hash))

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
            id_tuple = (task.node_id, int(task.task_args_hash))
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
                session.rollback()
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
                session.rollback()
                raise InvalidUsage(
                    "Task attributes are constrained to 255 characters, you may have values "
                    f"that are too long. Message: {str(e)}",
                    status_code=400,
                ) from e
        session.commit()

    resp = jsonify(tasks=return_tasks)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_attribute_type(
    names: Union[List[str], Set[str]],
    session: Session
) -> List[TaskAttributeType]:
    attribute_types = [{"name": name} for name in names]
    try:
        insert_stmt = add_ignore(insert(TaskAttributeType))
        session.execute(insert_stmt, attribute_types)
        session.flush()
    except DataError as e:
        # Attributes likely too long, message back
        session.rollback()
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


@finite_state_machine.route("/task/bind_resources", methods=["POST"])
def bind_task_resources() -> Any:
    """Add the task resources for a given task."""
    data = cast(Dict, request.get_json())

    with SessionLocal.begin() as session:

        new_resources = TaskResources(
            queue_id=data["queue_id"],
            task_resources_type_id=data.get("task_resources_type_id", None),
            requested_resources=data.get("requested_resources", None),
        )
        session.add(new_resources)
        session.commit()

    resp = jsonify(new_resources.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/task_status", methods=["GET"])
def get_task_status() -> Any:
    """Get the status of a task."""
    task_ids = request.args.getlist("task_ids")
    if len(task_ids) == 0:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)

    select_stmt = select(
        TaskInstance.id.label("TASK_INSTANCE_ID"),
        TaskInstance.distributor_id.label("DISTRIBUTOR_ID"),
        TaskInstanceStatus.label.label("STATUS"),
        TaskInstance.usage_str.label("RESOURCE_USAGE"),
        TaskInstance.stdout.label("STDOUT"),
        TaskInstance.stderr.label("STDERR"),
        TaskInstanceErrorLog.description.label("ERROR_TRACE")
    ).join_from(
        TaskInstance,
        TaskInstanceErrorLog,
        TaskInstance.id == TaskInstanceErrorLog.task_instance_id
    )
    where_clause = [Task.id.in_(task_ids)]
    # status is an optional arg
    status_request = request.args.getlist("status")
    if status_request:
        status_codes = [
            i
            for arg in status_request
            for i in _reversed_task_instance_label_mapping[arg]
        ]
        where_clause.append(TaskInstance.status.in_(status_codes))

    with SessionLocal.begin() as session:

        # serialize data for pandas. format is:
        #     {column -> {index -> value}}
        json: Dict[str, Dict[int, Any]] = {
            "TASK_INSTANCE_ID": {},
            "DISTRIBUTOR_ID": {},
            "STATUS": {},
            "RESOURCE_USAGE": {},
            "STDOUT": {},
            "STDERR": {},
            "ERROR_TRACE": {},
        }
        index = 0
        for row in session.execute(select_stmt.where(*where_clause)):
            for col in json.keys():
                if col == "STATUS":
                    json[col].update({index: _task_instance_label_mapping[row.col]})
                else:
                    json[col].update({index: row.col})
            index += 1

    resp = jsonify(task_instance_status=json)
    resp.status_code = StatusCodes.OK
    return resp


def _get_node_downstream(nodes: set, dag_id: int) -> Set[int]:
    """Get all downstream nodes of a node.

    Args:
        nodes (set): set of nodes
        dag_id (int): ID of DAG
    """
    with SessionLocal.begin() as session:
        select_stmt = select(
            Edge.downstream_node_ids
        ).where(
            Edge.dag_id == dag_id,
            Edge.node_id.in_(list(nodes))
        )
        result = session.execute(select_stmt).scalars().all()
    node_ids: Set[int] = set()
    for node in result:
        if node.downstream_node_ids is not None:
            ids = json.loads(node.downstream_node_ids)
            node_ids = node_ids.union(set(ids))
    return node_ids


def _get_node_uptream(nodes: set, dag_id: int) -> set:
    """Get all downstream nodes of a node.

    :param node_id:
    :return: a list of node_id
    """
    with SessionLocal.begin() as session:
        select_stmt = select(
            Edge.upstream_node_ids
        ).where(
            Edge.dag_id == dag_id,
            Edge.node_id.in_(list(nodes))
        )
        result = session.execute(select_stmt).scalars().all()
    node_ids: Set[int] = set()
    for node in result:
        if node.upstream_node_ids is not None:
            ids = json.loads(node.upstream_node_ids)
            node_ids = node_ids.union(set(ids))
    return node_ids


def _get_subdag(node_ids: list, dag_id: int) -> list:
    """Get all descendants of a given nodes.

    It only queries the primary keys on the edge table without join.

    Args:
        node_ids (list): list of node IDs
        dag_id (int): ID of DAG
    """
    node_set = set(node_ids)
    node_descendants = node_set
    while len(node_descendants) > 0:
        node_descendants = _get_node_downstream(node_descendants, dag_id)
        node_set = node_set.union(node_descendants)
    return list(node_set)


def _get_tasks_from_nodes(
    workflow_id: int,
    nodes: List,
    task_status: List,
) -> dict:
    """Get task ids of the given node ids.

    Args:
        workflow_id (int): ID of the workflow
        nodes (list): list of nodes
        task_status (list): list of task statuses
    """
    if nodes is None or len(nodes) == 0:
        return {}
    node_str = str((tuple(nodes))).replace(",)", ")")

    q = f"""
        SELECT id, status
        FROM task
        WHERE workflow_id={workflow_id}
        AND node_id in {node_str}
    """
    result = DB.session.execute(q).fetchall()
    task_dict = {}

    for r in result:
        # When task_status not specified, return the full subdag
        if len(task_status) == 0:
            task_dict[int(r[0])] = r[1]
        else:
            if r[1] in task_status:
                task_dict[int(r[0])] = r[1]
    return task_dict


@finite_state_machine.route("/task/subdag", methods=["POST"])
def get_task_subdag() -> Any:
    """Used to get the sub dag  of a given task.

    It returns a list of sub tasks as well as a list of sub nodes.
    """
    # Only return sub tasks in the following status. If empty or None, return all
    data = request.get_json()
    task_ids = data.get("task_ids", [])
    task_status = data.get("task_status", [])

    if len(task_ids) == 0:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)
    task_ids_str = "("
    for t in task_ids:
        task_ids_str += str(t) + ","
    task_ids_str = task_ids_str[:-1] + ")"
    if task_status is None:
        task_status = []

    q = f"""
        SELECT workflow.id as workflow_id, dag_id, node_id
        FROM task, workflow
        WHERE task.id in {task_ids_str} and task.workflow_id = workflow.id
    """
    result = DB.session.execute(q).fetchall()

    if result is None:
        # return empty values when task_id does not exist or db out of consistency
        resp = jsonify(workflow_id=None, sub_task=None)
        resp.status_code = StatusCodes.OK
        return resp

    # Since we have validated all the tasks belong to the same wf in status_command before
    # this call, assume they all belong to the same wf.
    workflow_id = result[0]["workflow_id"]
    dag_id = result[0]["dag_id"]
    node_ids = []
    for r in result:
        node_ids.append(r["node_id"])
    sub_dag_tree = _get_subdag(node_ids, dag_id)
    sub_task_tree = _get_tasks_from_nodes(workflow_id, sub_dag_tree, task_status)
    resp = jsonify(workflow_id=workflow_id, sub_task=sub_task_tree)

    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/task/update_statuses", methods=["PUT"])
def update_task_statuses() -> Any:
    """Update the status of the tasks."""
    data = request.get_json()
    try:
        task_ids = data["task_ids"]
        new_status = data["new_status"]
        workflow_status = data["workflow_status"]
        workflow_id = data["workflow_id"]
    except KeyError as e:
        raise InvalidUsage(
            f"problem with {str(e)} in request to {request.path}", status_code=400
        ) from e

    task_ids_str = "(" + ",".join([str(i) for i in task_ids]) + ")"
    try:
        task_q = """
            UPDATE task
            SET status = '{new_status}'
            WHERE id IN {task_ids}
        """.format(
            new_status=new_status, task_ids=task_ids_str
        )

        task_res = DB.session.execute(task_q)
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    try:
        # If job is supposed to be rerun, set task instances to "K"
        if new_status == TaskStatus.REGISTERING:
            task_instance_q = """
                UPDATE task_instance
                SET status = '{k_code}'
                WHERE task_id in {task_ids}
            """.format(
                k_code=TaskInstanceStatus.KILL_SELF, task_ids=task_ids_str
            )
            DB.session.execute(task_instance_q)

            # If workflow is done, need to set it to an error state before resume
            if workflow_status == Statuses.DONE:
                workflow_q = """
                    UPDATE workflow
                    SET status = '{status}'
                    WHERE id = {workflow_id}
                """.format(
                    status=Statuses.FAILED, workflow_id=workflow_id
                )
                DB.session.execute(workflow_q)

        DB.session.commit()
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    message = f"{task_res.rowcount} rows updated to status {new_status}"
    resp = jsonify(message)
    resp.status_code = StatusCodes.OK
    return resp


def _get_dag_and_wf_id(task_id: int) -> tuple:
    q = f"""
            SELECT dag_id, workflow_id, node_id
            FROM task, workflow
            WHERE task.workflow_id = workflow.id
            AND task.id = {task_id}
        """
    row = DB.session.execute(q).fetchone()

    if row is None:
        return None, None, None
    return int(row["dag_id"]), int(row["workflow_id"]), int(row["node_id"])


@finite_state_machine.route("/task_dependencies/<task_id>", methods=["GET"])
def get_task_dependencies(task_id: int) -> Any:
    """Get task's downstream and upsteam tasks and their status."""
    dag_id, workflow_id, node_id = _get_dag_and_wf_id(task_id)
    up_nodes = _get_node_uptream({node_id}, dag_id)
    down_nodes = _get_node_downstream({node_id}, dag_id)
    up_task_dict = _get_tasks_from_nodes(workflow_id, list(up_nodes), [])
    down_task_dict = _get_tasks_from_nodes(workflow_id, list(down_nodes), [])
    # return a "standard" json format so that it can be reused by future GUI
    up = (
        []
        if up_task_dict is None or len(up_task_dict) == 0
        else [[{"id": k, "status": up_task_dict[k]}] for k in up_task_dict][0]
    )
    down = (
        []
        if down_task_dict is None or len(down_task_dict) == 0
        else [[{"id": k, "status": down_task_dict[k]}] for k in down_task_dict][0]
    )
    resp = jsonify({"up": up, "down": down})
    resp.status_code = 200
    return resp


@finite_state_machine.route("/tasks_recursive/<direction>", methods=["PUT"])
def get_tasks_recursive(direction: str) -> Any:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    dir = Direction.UP if direction == "up" else Direction.DOWN
    data = request.get_json()
    # define task_ids as set in order to eliminate dups
    task_ids = set(data.get("task_ids", []))

    try:
        tasks_recursive = _get_tasks_recursive(task_ids, dir)
        resp = jsonify({"task_ids": list(tasks_recursive)})
        resp.status_code = 200
        return resp
    except InvalidUsage as e:
        logger.info(f"InvalidUsage {e} is encountered!")
        raise e


def _get_tasks_recursive(task_ids: Set[int], direction: str) -> set:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    tasks_recursive = set()
    next_nodes = set()
    _workflow_id_first = None
    for task_id in task_ids:
        dag_id, workflow_id, node_id = _get_dag_and_wf_id(task_id)
        next_nodes_sub = (
            _get_node_downstream({node_id}, dag_id)
            if direction == Direction.DOWN
            else _get_node_uptream({node_id}, dag_id)
        )
        if _workflow_id_first is None:
            workflow_id_first = workflow_id
        elif workflow_id != workflow_id_first:
            raise InvalidUsage(
                f"{task_ids} in request belong to different workflow_ids"
                f"({workflow_id_first}, {workflow_id})",
                status_code=400,
            )
        next_nodes.update(next_nodes_sub)

    if len(next_nodes) > 0:
        next_task_dict = _get_tasks_from_nodes(workflow_id_first, list(next_nodes), [])
        if len(next_task_dict) > 0:
            task_recursive_sub = _get_tasks_recursive(
                set(next_task_dict.keys()), direction
            )
            tasks_recursive.update(task_recursive_sub)

    tasks_recursive.update(task_ids)

    return tasks_recursive


@finite_state_machine.route("/task_resource_usage", methods=["GET"])
def get_task_resource_usage() -> Any:
    """Return the resource usage for a given Task ID."""
    try:
        task_id = request.args["task_id"]
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to /task_resource_usage", status_code=400
        ) from e

    query = """
        SELECT
            task.num_attempts,
            task_instance.nodename,
            task_instance.wallclock,
            task_instance.maxpss
        FROM
            task
        JOIN
            task_instance
        ON
            task.id = task_instance.task_id
        WHERE
            task_id = :task_id AND task_instance.status = 'D'
    """

    result = (
        DB.session.query(
            Task.num_attempts,
            TaskInstance.nodename,
            TaskInstance.wallclock,
            TaskInstance.maxpss,
        )
        .from_statement(text(query))
        .params(task_id=task_id)
        .one_or_none()
    )

    DB.session.commit()

    if result is None:
        resource_usage = SerializeTaskResourceUsage.to_wire(None, None, None, None)
    else:
        resource_usage = SerializeTaskResourceUsage.to_wire(
            result.num_attempts, result.nodename, result.wallclock, result.maxpss
        )
    resp = jsonify(resource_usage)
    resp.status_code = StatusCodes.OK
    return resp
