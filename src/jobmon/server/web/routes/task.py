"""Routes for Tasks"""
import json
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Set, Union


from flask import current_app as app, jsonify, request

from jobmon.constants import TaskInstanceStatus, TaskStatus, WorkflowStatus as Statuses
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.server_side_exception import InvalidUsage

import pandas as pd

from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import text

from . import jobmon_cli, jobmon_client, jobmon_swarm

_task_instance_label_mapping = {
    "B": "PENDING",
    "I": "PENDING",
    "R": "RUNNING",
    "E": "FATAL",
    "Z": "FATAL",
    "W": "FATAL",
    "U": "FATAL",
    "K": "FATAL",
    "D": "DONE"
}

_reversed_task_instance_label_mapping = {
    "PENDING": ["B", "I"],
    "RUNNING": ["R"],
    "FATAL": ["E", "Z", "W", "U", "K"],
    "DONE": ["D"]
}


@jobmon_client.route('/task', methods=['GET'])
def get_task_id_and_status():
    """Get the status and id of a Task."""
    try:
        wid = request.args['workflow_id']
        int(wid)
        nid = request.args['node_id']
        int(nid)
        h = request.args["task_args_hash"]
        int(h)
        app.logger = app.logger.bind(workflow_id=wid, node_id=nid, task_args_hash=str(h))
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    query = """
        SELECT task.id, task.status
        FROM task
        WHERE
            workflow_id = :workflow_id
            AND node_id = :node_id
            AND task_args_hash = :task_args_hash
    """
    result = DB.session.query(Task).from_statement(text(query)).params(
        workflow_id=wid,
        node_id=nid,
        task_args_hash=h
    ).one_or_none()

    # send back json
    if result is None:
        resp = jsonify({'task_id': None, 'task_status': None})
        app.logger.info(f"The task_id for wf {wid},  node_id {nid}, and task_args_hash {h} "
                        f"is none.")
    else:
        resp = jsonify({'task_id': result.id, 'task_status': result.status})
        app.logger.info(f"The task_id for wf {wid},  node_id {nid}, and task_args_hash {h} "
                        f"is {result.id}.")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/task', methods=['POST'])
def add_task():
    """Add a task to the database.

    Args:
        a list of:
            workflow_id: workflow this task is associated with
            node_id: structural node this task is associated with
            task_arg_hash: hash of the data args for this task
            name: task's name
            command: task's command
            max_attempts: how many times the job should be attempted
            task_args: dictionary of data args for this task
            task_attributes: dictionary of attributes associated with the task
    """
    try:
        data = request.get_json()
        app.logger.debug(data)
        ts = data.pop("tasks")
        # build a hash table for ts
        ts_ht = {}  # {<node_id::task_arg_hash>, task}
        tasks = []
        task_args = []
        task_attribute_list = []

        for t in ts:
            # input variable check
            int(t["workflow_id"])
            int(t["node_id"])
            ts_ht[str(t["node_id"]) + "::" + str(t["task_args_hash"])] = t
            task = Task(
                workflow_id=t["workflow_id"],
                node_id=t["node_id"],
                task_args_hash=t["task_args_hash"],
                name=t["name"],
                command=t["command"],
                max_attempts=t["max_attempts"],
                status=TaskStatus.REGISTERED)
            tasks.append(task)
        DB.session.add_all(tasks)
        DB.session.flush()
        for task in tasks:
            t = ts_ht[str(task.node_id) + "::" + str(task.task_args_hash)]
            for _id, val in t["task_args"].items():
                task_arg = TaskArg(task_id=task.id, arg_id=_id, val=val)
                task_args.append(task_arg)

            if t["task_attributes"]:
                for name, val in t["task_attributes"].items():
                    type_id = _add_or_get_attribute_type([name]).id
                    task_attribute = TaskAttribute(task_id=task.id,
                                                   task_attribute_type_id=type_id,
                                                   value=val)
                    task_attribute_list.append(task_attribute)
        DB.session.add_all(task_args)
        DB.session.flush()
        DB.session.add_all(task_attribute_list)
        DB.session.flush()
        DB.session.commit()
        # return value

        return_dict = {}  # {<name>: <id>}
        for t in tasks:
            return_dict[t.name] = t.id
        resp = jsonify(tasks=return_dict)
        resp.status_code = StatusCodes.OK
        return resp
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    except TypeError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e


@jobmon_client.route('/task/<task_id>/update_parameters', methods=['PUT'])
def update_task_parameters(task_id: int):
    """Update the parameters for a given task."""
    app.logger = app.logger.bind(task_id=task_id)
    data = request.get_json()
    app.logger.info(f"Update task {task_id} parameters")

    try:
        int(task_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    query = """SELECT task.* FROM task WHERE task.id = :task_id"""
    task = DB.session.query(Task).from_statement(text(query)).params(
        task_id=task_id).one()
    task.reset(name=data["name"], command=data["command"],
               max_attempts=data["max_attempts"],
               reset_if_running=data["reset_if_running"])

    for name, val in data["task_attributes"].items():
        _add_or_update_attribute(task_id, name, val)
        DB.session.flush()

    DB.session.commit()

    resp = jsonify(task_status=task.status)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/task/bind_tasks', methods=['PUT'])
def bind_tasks():
    """Bind the task objects to the database."""
    all_data = request.get_json()
    tasks = all_data["tasks"]
    workflow_id = int(all_data["workflow_id"])
    app.logger = app.logger.bind(workflow_id=workflow_id)
    app.logger.info(f"Binding tasks for wf {workflow_id}")
    # receive from client the tasks in a format of:
    # {<hash>:[node_id(1), task_args_hash(2), name(3), command(4), max_attempts(5),
    # reset_if_running(6), task_args(7),task_attributes(8)]}

    # Retrieve existing task_ids
    task_query = """
        SELECT task.id, task.node_id, task.task_args_hash, task.status
        FROM task
        WHERE workflow_id = {workflow_id}
        AND (task.node_id, task.task_args_hash) IN ({tuples})
    """

    task_query_params = ','.join([f"({task[0]},{task[1]})" for task in tasks.values()])

    prebound_task_query = task_query.format(workflow_id=workflow_id,
                                            tuples=task_query_params)
    prebound_tasks = DB.session.query(Task).from_statement(text(prebound_task_query)).all()

    # Bind tasks not present in DB
    tasks_to_add: List[Dict] = []  # Container for tasks not yet bound to the database
    tasks_to_update = 0
    present_tasks = {
        (task.node_id, int(task.task_args_hash)): task
        for task in prebound_tasks
    }  # Dictionary mapping existing Tasks to the supplied arguments

    arg_attr_mapping = {}  # Dict mapping input tasks to the corresponding args/attributes
    task_hash_lookup = {}  # Reverse dictionary of inputs, maps hash back to values

    for hashval, items in tasks.items():

        node_id, arg_hash, name, command, max_att, reset, args, attrs = items

        id_tuple = (node_id, int(arg_hash))

        # Conditional logic: Has task already been bound to the DB? If yes, reset the
        # task status and update the args/attributes
        if id_tuple in present_tasks.keys():
            task = present_tasks[id_tuple]
            task.reset(name=name,
                       command=command,
                       max_attempts=max_att,
                       reset_if_running=reset)
            tasks_to_update += 1

        # If not, add the task
        else:
            task = {
                'workflow_id': workflow_id,
                'node_id': node_id,
                'task_args_hash': arg_hash,
                'name': name,
                'command': command,
                'max_attempts': max_att,
                'status': TaskStatus.REGISTERED
            }
            tasks_to_add.append(task)

        arg_attr_mapping[hashval] = (args, attrs)
        task_hash_lookup[id_tuple] = hashval

    # Update existing tasks
    if tasks_to_update > 0:

        # ORM task objects already updated in task.reset, flush the changes
        DB.session.flush()

    # Bind new tasks with raw SQL
    if len(tasks_to_add):

        DB.session.execute(insert(Task), tasks_to_add)
        DB.session.flush()

        # Fetch newly bound task ids
        new_task_params = ','.join(
            [f"({task['node_id']},{task['task_args_hash']})" for task in tasks_to_add]
        )
        new_task_query = task_query.format(workflow_id=workflow_id, tuples=new_task_params)

        new_tasks = DB.session.query(Task).from_statement(text(new_task_query)).all()

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
        task_attributes_types = _add_or_get_attribute_type(attr_names)

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
            task_arg = {'task_id': task_id, 'arg_id': key, 'val': val}
            args_to_add.append(task_arg)

        for name, val in attrs.items():
            attr_type_id = task_attr_type_mapping[name]
            insert_vals = {
                'task_id': task_id,
                'task_attribute_type_id': attr_type_id,
                'value': val}
            attrs_to_add.append(insert_vals)

    if args_to_add:
        arg_insert_stmt = insert(TaskArg).values(args_to_add).on_duplicate_key_update(
            val=text("VALUES(val)"))
        DB.session.execute(arg_insert_stmt)
    if attrs_to_add:
        attr_insert_stmt = insert(TaskAttribute).values(attrs_to_add).\
            on_duplicate_key_update(value=text("VALUES(value)"))
        DB.session.execute(attr_insert_stmt)
    DB.session.commit()

    resp = jsonify(tasks=return_tasks)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_attribute_type(names: Union[List[str], Set[str]]) -> List[TaskAttributeType]:
    attribute_types = [{'name': name} for name in names]
    insert_stmt = insert(TaskAttributeType).prefix_with("IGNORE")
    DB.session.execute(insert_stmt, attribute_types)
    DB.session.commit()

    # Query the IDs
    attribute_type_ids = DB.session.query(TaskAttributeType).filter(
        TaskAttributeType.name.in_(names)).all()
    return attribute_type_ids


def _add_or_update_attribute(task_id: int, name: str, value: str) -> int:
    attribute_type = _add_or_get_attribute_type(name)
    insert_vals = insert(TaskAttribute).values(
        task_id=task_id,
        task_attribute_type_id=attribute_type,
        value=value
    )
    update_insert = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value
    )
    attribute = DB.session.execute(update_insert)
    DB.session.commit()
    return attribute.id


@jobmon_client.route('/task/<task_id>/task_attributes', methods=['PUT'])
def update_task_attribute(task_id: int):
    """Add or update attributes for a task."""
    app.logger = app.logger.bind(task_id=task_id)
    try:
        int(task_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    data = request.get_json()
    app.logger.info(f"Updating task attributes for task {task_id}")
    attributes = data["task_attributes"]
    # update existing attributes with their values
    for name, val in attributes.items():
        _add_or_update_attribute(task_id, name, val)
    # Flask requires that a response is returned, no values need to be passed back
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/task/<task_id>/queue', methods=['POST'])
def queue_job(task_id: int):
    """Queue a job and change its status
    Args:

        job_id: id of the job to queue
    """
    app.logger = app.logger.bind(task_id=task_id)
    app.logger.debug(f"Queue job {task_id}")
    task = DB.session.query(Task).filter_by(id=task_id).one()
    try:
        task.transition(TaskStatus.QUEUED_FOR_INSTANTIATION)
    except InvalidStateTransition:
        # Handles race condition if the task has already been queued
        if task.status == TaskStatus.QUEUED_FOR_INSTANTIATION:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   f"{task_id} from Q to Q")
            app.logger.warning(msg)
        else:
            raise
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


def _transform_mem_to_gb(mem_str: Any) -> float:
    # we allow both upper and lowercase g, m, t options
    # BUG g and G are not the same
    if mem_str is None:
        return 2
    if type(mem_str) in (float, int):
        return mem_str
    if mem_str[-1].lower() == "m":
        mem = float(mem_str[:-1])
        mem /= 1000
    elif mem_str[-2:].lower() == "mb":
        mem = float(mem_str[:-2])
        mem /= 1000
    elif mem_str[-1].lower() == "t":
        mem = float(mem_str[:-1])
        mem *= 1000
    elif mem_str[-2:].lower() == "tb":
        mem = float(mem_str[:-2])
        mem *= 1000
    elif mem_str[-1].lower() == "g":
        mem = float(mem_str[:-1])
    elif mem_str[-2:].lower() == "gb":
        mem = float(mem_str[:-2])
    else:
        mem = 1
    return mem


@jobmon_swarm.route('/task/<task_id>/bind_resources', methods=['POST'])
def bind_task_resources(task_id: int):
    """Add the task resources for a given task

    Args:
        task_id (int): id of the task for which task resources will be added
    """
    app.logger = app.logger.bind(task_id=task_id)
    data = request.get_json()

    try:
        task_id_int = int(task_id)
    except ValueError:
        resp = jsonify(msg="task_id {} is not a number".format(task_id))
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp

    new_resources = TaskResources(
        task_id=task_id_int,
        queue_id=data.get('queue_id', None),
        task_resources_type_id=data.get('task_resources_type_id', None),
        resource_scales=data.get('resource_scales', None),
        requested_resources=data.get('requested_resources', None))
    DB.session.add(new_resources)
    DB.session.flush()  # get auto increment
    new_resources.activate()
    DB.session.commit()

    resp = jsonify(new_resources.id)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/task/<task_id>/update_resources', methods=['POST'])
def update_task_resources(task_id: int):
    """Change the resources set for a given task

    Args:
        task_id (int): id of the task for which resources will be changed
        parameter_set_type (str): parameter set type for this task
        max_runtime_seconds (int, optional): amount of time task is allowed to
            run for
        context_args (dict, optional): unstructured parameters to pass to
            executor
        resource_scales (dict): values to scale by upon resource error
        hard_limit (bool): whether to move queues if requester resources exceed
            queue limits
    """
    app.logger = app.logger.bind(task_id=task_id)
    data = request.get_json()
    app.logger.info("Update task resource for {task_id}")
    task_resources_type_id = data.get('task_resources_type_id')

    try:
        task_id = int(task_id)
    except ValueError:
        resp = jsonify(msg="task_id {} is not a number".format(task_id))
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp

    resources = TaskResources(
        task_id=task_id,
        task_resources_type_id=task_resources_type_id,
        resource_scales=data.get('resource_scales', None))
    DB.session.add(resources)
    DB.session.flush()  # get auto increment
    resources.activate()
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/task_status', methods=['GET'])
def get_task_status():
    """Get the status of a task."""
    task_ids = request.args.getlist('task_ids')
    if len(task_ids) == 0:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)
    params = {'task_ids': task_ids}
    where_clause = "task.id IN :task_ids"

    # status is an optional arg
    status_request = request.args.getlist('status', None)
    if len(status_request) > 0:
        status_codes = [i for arg in status_request
                        for i in _reversed_task_instance_label_mapping[arg]]
        params['status'] = status_codes
        where_clause += " AND task_instance.status IN :status"
    q = """
        SELECT
            task.id AS TASK_ID,
            task.status AS task_status,
            task_instance.id AS TASK_INSTANCE_ID,
            executor_id AS EXECUTOR_ID,
            task_instance_status.label AS STATUS,
            usage_str AS RESOURCE_USAGE,
            description AS ERROR_TRACE
        FROM task
        JOIN task_instance
            ON task.id = task_instance.task_id
        JOIN task_instance_status
            ON task_instance.status = task_instance_status.id
        JOIN executor_parameter_set
            ON task_instance.executor_parameter_set_id = executor_parameter_set.id
        LEFT JOIN task_instance_error_log
            ON task_instance.id = task_instance_error_log.task_instance_id
        WHERE
            {where_clause}""".format(where_clause=where_clause)
    res = DB.session.execute(q, params).fetchall()

    if res:
        # assign to dataframe for serialization
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_task_instance_label_mapping, inplace=True)
        df = df[["TASK_INSTANCE_ID", "EXECUTOR_ID", "STATUS", "RESOURCE_USAGE",
                 "ERROR_TRACE"]]
        resp = jsonify(task_instance_status=df.to_json())
    else:
        df = pd.DataFrame(
            {},
            columns=["TASK_INSTANCE_ID", "EXECUTOR_ID", "STATUS",
                     "RESOURCE_USAGE", "ERROR_TRACE"])
        resp = jsonify(task_instance_status=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


def _get_node_downstream(nodes: set, dag_id: int) -> set:
    """
    Get all downstream nodes of a node
    :param node_id:
    :return: a list of node_id
    """
    nodes_str = str((tuple(nodes))).replace(",)", ")")
    q = f"""
        SELECT downstream_node_ids
        FROM edge
        WHERE dag_id = {dag_id}
        AND node_id in {nodes_str}
    """
    result = DB.session.execute(q).fetchall()

    if result is None or len(result) == 0:
        return []
    node_ids = set()
    for r in result:
        if r['downstream_node_ids'] is not None:
            ids = json.loads(r['downstream_node_ids'])
            node_ids = node_ids.union(set(ids))
    return node_ids


def _get_subdag(node_ids: list, dag_id: int) -> list:
    """
    Get all descendants of a given nodes. It only queries the primary keys on the edge table
    without join.
    :param node_ids:
    :return: a list of node_id
    """
    node_set = set(node_ids)
    node_descendants = node_set
    while len(node_descendants) > 0:
        node_descendants = _get_node_downstream(node_descendants, dag_id)
        node_set = node_set.union(node_descendants)
    return list(node_set)


def _get_tasks_from_nodes(workflow_id: int, nodes: list, task_status: list) -> dict:
    """
    Get task ids of the given node ids
    :param workflow_id:
    :param nodes:
    :return: a dict of {<id>: <status>}
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


@jobmon_cli.route('/task/subdag', methods=['POST'])
def get_task_subdag():
    """
    Used to get the sub dag  of a given task. It returns a list of sub tasks as well as a
    list of sub nodes.
    :return:
    """
    # Only return sub tasks in the following status. If empty or None, return all
    data = request.get_json()
    task_ids = data.get('task_ids', [])
    task_status = data.get('task_status', [])

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
    workflow_id = result[0]['workflow_id']
    dag_id = result[0]['dag_id']
    node_ids = []
    for r in result:
        node_ids.append(r['node_id'])
    sub_dag_tree = _get_subdag(node_ids, dag_id)
    sub_task_tree = _get_tasks_from_nodes(workflow_id, sub_dag_tree, task_status)
    resp = jsonify(workflow_id=workflow_id, sub_task=sub_task_tree)

    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/task/update_statuses', methods=['PUT'])
def update_task_statuses():
    """Update the status of the tasks."""
    data = request.get_json()
    try:
        task_ids = data['task_ids']
        new_status = data['new_status']
        workflow_status = data['workflow_status']
        workflow_id = data['workflow_id']
    except KeyError as e:
        raise InvalidUsage(f"problem with {str(e)} in request to {request.path}",
                           status_code=400) from e

    task_ids_str = '(' + ','.join([str(i) for i in task_ids]) + ')'
    try:
        task_q = """
            UPDATE task
            SET status = '{new_status}'
            WHERE id IN {task_ids}
        """.format(new_status=new_status, task_ids=task_ids_str)

        task_res = DB.session.execute(task_q)
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    try:
        # If job is supposed to be rerun, set task instances to "K"
        if new_status == TaskStatus.REGISTERED:
            task_instance_q = """
                UPDATE task_instance
                SET status = '{k_code}'
                WHERE task_id in {task_ids}
            """.format(k_code=TaskInstanceStatus.KILL_SELF, task_ids=task_ids_str)
            DB.session.execute(task_instance_q)

            # If workflow is done, need to set it to an error state before resume
            if workflow_status == Statuses.DONE:
                workflow_q = """
                    UPDATE workflow
                    SET status = '{status}'
                    WHERE id = {workflow_id}
                """.format(status=Statuses.FAILED, workflow_id=workflow_id)
                DB.session.execute(workflow_q)

        DB.session.commit()
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    message = f"{task_res.rowcount} rows updated to status {new_status}"
    resp = jsonify(message)
    resp.status_code = StatusCodes.OK
    return resp
