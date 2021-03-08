import json
import os
from http import HTTPStatus as StatusCodes
from typing import Dict, List, Set, Union


from flask import Blueprint, current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.arg_type import ArgType
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_instance import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.server_side_exception import (InvalidUsage, ServerError)

import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import func, text

from . import jobmon_client, jobmon_cli


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
        app.logger = app.logger.bind(workflow_id=wid, node_id=nid, task_args_hash=h)
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
    else:
        resp = jsonify({'task_id': result.id, 'task_status': result.status})
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
    attributes = data["task_attributes"]
    # update existing attributes with their values
    for name, val in attributes.items():
        _add_or_update_attribute(task_id, name, val)
    # Flask requires that a response is returned, no values need to be passed back
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
