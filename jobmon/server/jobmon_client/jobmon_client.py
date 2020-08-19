from http import HTTPStatus as StatusCodes
import os
from datetime import datetime, timedelta
import json
from typing import Optional, Dict, Any

from flask import jsonify, request, Blueprint, current_app as app
from werkzeug.local import LocalProxy
from sqlalchemy.sql import func, text
from sqlalchemy.dialects.mysql import insert
import sqlalchemy

from jobmon import config
from jobmon.models import DB
from jobmon.models.arg import Arg
from jobmon.models.arg_type import ArgType
from jobmon.models.constants import qsub_attribute, task_instance_attribute
from jobmon.models.task_attribute import TaskAttribute
from jobmon.models.task_attribute_type import TaskAttributeType
from jobmon.models.workflow_attribute import WorkflowAttribute
from jobmon.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.models.command_template_arg_type_mapping import \
    CommandTemplateArgTypeMapping
from jobmon.models.dag import Dag
from jobmon.models.edge import Edge
from jobmon.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.node import Node
from jobmon.models.node_arg import NodeArg
from jobmon.models.task import Task
from jobmon.models.task_arg import TaskArg
from jobmon.models.task_instance import TaskInstance
from jobmon.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.models.task_instance import TaskInstanceStatus
from jobmon.models.task_status import TaskStatus
from jobmon.models.task_template import TaskTemplate
from jobmon.models.task_template_version import TaskTemplateVersion
from jobmon.models.tool import Tool
from jobmon.models.tool_version import ToolVersion
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.models.workflow_run import WorkflowRun
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.server_side_exception import log_and_raise, raise_user_error, InvalidUsage, ServerError

jobmon_client = Blueprint("jobmon_client", __name__)


logger = LocalProxy(lambda: app.logger)


@jobmon_client.errorhandler(404)
def page_not_found(error):
    return 'This route does not exist {}'.format(request.url), 404


# error handling
@jobmon_client.errorhandler(InvalidUsage)
def handle_40x(error):
    response = jsonify(error.to_dict())
    response.status_code = 400
    return response


@jobmon_client.errorhandler(ServerError)
def handle_50x(error):
    response = jsonify(error.to_dict())
    response.status_code = 500
    return response


@jobmon_client.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening
    """
    logger.info(f"{os.getpid()}: {jobmon_client.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route("/time", methods=['GET'])
def get_pst_now():
    try:
        time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
        time = time['time']
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        DB.session.commit()
        resp = jsonify(time=time)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)


@jobmon_client.route("/health", methods=['GET'])
def health():
    """
    Test connectivity to the database, return 200 if everything is ok
    Defined in each module with a different route, so it can be checked individually
    """
    try:
        time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
        time = time['time']
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        DB.session.commit()
        # Assume that if we got this far without throwing an exception, we should be online
        resp = jsonify(status='OK')
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)


@jobmon_client.route('/tool', methods=['POST'])
def add_tool():
    """Add a tool to the database"""
    # input variable check
    data = request.get_json()
    try:
        name = data["name"]
    except KeyError as e:
        raise_user_error("Parameter name is missing", app.logger)
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)

    # add tool to db
    try:
        tool = Tool(name=name)
        DB.session.add(tool)
        DB.session.commit()
        tool = tool.to_wire_as_client_tool()
        resp = jsonify(tool=tool)
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError as e:
        DB.session.rollback()
        tool = None
        resp = jsonify(tool=tool)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)



@jobmon_client.route('/tool/<tool_name>', methods=['GET'])
def get_tool(tool_name: str):
    # check input variable
    if tool_name is None:
        raise_user_error("Variable tool_name is None", app.logger)

    # get data from db
    try:
        query = """
            SELECT
                tool.*
            FROM
                tool
            WHERE
                name = :tool_name"""
        tool = DB.session.query(Tool).from_statement(text(query)).params(
            tool_name=tool_name
        ).one_or_none()
        DB.session.commit()
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)
    if tool:
        try:
            tool = tool.to_wire_as_client_tool()
            resp = jsonify(tool=tool)
            resp.status_code = StatusCodes.OK
            return resp
        except Exception as e:
            log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)
    else:
        raise_user_error("Tool {} does not exist in DB".format(tool_name), app.logger)



@jobmon_client.route('/tool/<tool_id>/tool_versions', methods=['GET'])
def get_tool_versions(tool_id: int):
    # check input variable
    if tool_id is None:
        raise_user_error("Variable tool_id is None", app.logger)
    try:
        int(tool_id)
    except Exception as e:
        raise_user_error("Variable tool_id must be int", app.logger)

    # get data from db
    try:
        query = """
            SELECT
                tool_version.*
            FROM
                tool_version
            WHERE
                tool_id = :tool_id"""
        tool_versions = DB.session.query(ToolVersion).from_statement(text(query)).params(
            tool_id=tool_id
        ).all()
        DB.session.commit()
        tool_versions = [t.to_wire_as_client_tool_version() for t in tool_versions]
        resp = jsonify(tool_versions=tool_versions)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)


@jobmon_client.route('/tool_version', methods=['POST'])
def add_tool_version():
    # check input variable
    data = request.get_json()
    try:
        tool_id = int(data["tool_id"])
    except KeyError:
        raise_user_error("Parameter tool_id is missing", app.logger)
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)
    try:
        tool_version = ToolVersion(tool_id=tool_id)
        DB.session.add(tool_version)
        DB.session.commit()
        tool_version = tool_version.to_wire_as_client_tool_version()
        resp = jsonify(tool_version=tool_version)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)


@jobmon_client.route('/task_template', methods=['GET'])
def get_task_template():
    # parse args
    try:
        tool_version_id = request.args.get("tool_version_id")
        name = request.args.get("task_template_name")
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        # get data from db
        query = """
            SELECT
                task_template.*
            FROM task_template
            WHERE
                tool_version_id = :tool_version_id
                AND name = :name
        """
        tt = DB.session.query(TaskTemplate).from_statement(text(query)).params(
            tool_version_id=tool_version_id,
            name=name
        ).one_or_none()
        if tt is not None:
            task_template_id = tt.id
        else:
            task_template_id = None

        resp = jsonify(task_template_id=task_template_id)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)


@jobmon_client.route('/task_template', methods=['POST'])
def add_task_template():
    """Add a tool to the database"""
    # check input variable
    data = request.get_json()
    try:
        int(data["tool_version_id"])
    except KeyError as e:
        raise_user_error("Missing variable tool_version_id", app.logger)
    except Exception as e:
        raise_user_error(str(e), app.logger)

    # add to DB
    try:
        tt = TaskTemplate(tool_version_id=data["tool_version_id"],
                          name=data["task_template_name"])
        DB.session.add(tt)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM task_template
            WHERE
                tool_version_id = :tool_version_id
                AND name = :name
        """
        tt = DB.session.query(TaskTemplate).from_statement(text(query)).params(
            tool_version_id=data["tool_version_id"],
            name=data["task_template_name"]
        ).one()
        DB.session.commit()
    except Exception as e:
        log_and_raise("Unexpected jobmon server error: {}".format(e), app.logger)
    resp = jsonify(task_template_id=tt.id)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/task_template/<task_template_id>/version', methods=['GET'])
def get_task_template_version(task_template_id: int):
    # parse args
    if task_template_id is None:
        raise_user_error("Missing variable task_template_id", app.logger)
    try:
        int(task_template_id)
        command_template = request.args.get("command_template")
        arg_mapping_hash = request.args.get("arg_mapping_hash")
    except Exception as e:
        raise_user_error(str(e), app.logger)

    # get task template version object
    try:
        query = """
            SELECT
                task_template_version.*
            FROM task_template_version
            WHERE
                task_template_id = :task_template_id
                AND arg_mapping_hash = :arg_mapping_hash
                AND command_template = :command_template
        """
        ttv = DB.session.query(TaskTemplateVersion).from_statement(text(query)).params(
                task_template_id=task_template_id,
                command_template=command_template,
                arg_mapping_hash=arg_mapping_hash
        ).one_or_none()

        if ttv is not None:
            wire_obj = ttv.to_wire_as_client_task_template_version()
        else:
            wire_obj = None

        resp = jsonify(task_template_version=wire_obj)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


def _add_or_get_arg(name: str) -> Arg:
    try:
        arg = Arg(name=name)
        DB.session.add(arg)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM arg
            WHERE name = :name
        """
        arg = DB.session.query(Arg).from_statement(text(query)).params(name=name).one()
        DB.session.commit()
    return arg


@jobmon_client.route('/task_template/<task_template_id>/add_version', methods=['POST'])
def add_task_template_version(task_template_id: int):
    """Add a tool to the database"""
    # check input variables
    data = request.get_json()
    if task_template_id is None:
        raise_user_error("Missing variable task_template_id", app.logger)
    try:
        int(task_template_id)
        # populate the argument table
        arg_mapping_dct: dict = {ArgType.NODE_ARG: [],
                                 ArgType.TASK_ARG: [],
                                 ArgType.OP_ARG: []}
        for arg_name in data["node_args"]:
            arg_mapping_dct[ArgType.NODE_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["task_args"]:
            arg_mapping_dct[ArgType.TASK_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["op_args"]:
            arg_mapping_dct[ArgType.OP_ARG].append(_add_or_get_arg(arg_name))
    except Exception as e:
        raise_user_error(str(e), app.logger)

    try:
        ttv = TaskTemplateVersion(task_template_id=task_template_id,
                                  command_template=data["command_template"],
                                  arg_mapping_hash=data["arg_mapping_hash"])
        DB.session.add(ttv)
        DB.session.commit()

        # get a lock
        DB.session.refresh(ttv, with_for_update=True)

        for arg_type_id in arg_mapping_dct.keys():
            for arg in arg_mapping_dct[arg_type_id]:
                ctatm = CommandTemplateArgTypeMapping(
                    task_template_version_id=ttv.id,
                    arg_id=arg.id,
                    arg_type_id=arg_type_id)
                DB.session.add(ctatm)
        DB.session.commit()
        resp = jsonify(
            task_template_version=ttv.to_wire_as_client_task_template_version()
        )
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        # if another process is adding this task_template_version then this query should block
        # until the command_template_arg_type_mapping has been populated and committed
        query = """
            SELECT *
            FROM task_template_version
            WHERE
                task_template_id = :task_template_id
                AND command_template = :command_template
                AND arg_mapping_hash = :arg_mapping_hash
        """
        ttv = DB.session.query(TaskTemplateVersion).from_statement(text(query)).params(
            task_template_id=task_template_id,
            command_template=data["command_template"],
            arg_mapping_hash=data["arg_mapping_hash"]
        ).one()
        DB.session.commit()
        resp = jsonify(
            task_template_version=ttv.to_wire_as_client_task_template_version()
        )
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)



@jobmon_client.route('/node', methods=['GET'])
def get_node_id():
    """Get a node id: If a matching node isn't found, return None.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a node
        task_template_version_id: version id of the task_template a node
                                  belongs to.
    """
    try:
        query = """
            SELECT node.id
            FROM node
            WHERE
                node_args_hash = :node_args_hash
                AND task_template_version_id = :task_template_version_id"""
        result = DB.session.query(Node).from_statement(text(query)).params(
            node_args_hash=request.args['node_args_hash'],
            task_template_version_id=request.args['task_template_version_id']
        ).one_or_none()

        if result is None:
            resp = jsonify({'node_id': None})
        else:
            resp = jsonify({'node_id': result.id})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/node', methods=['POST'])
def add_node():
    """Add a new node to the database.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a
                        node.
        task_template_version_id: version id of the task_template a node
                                  belongs to.
        node_args: key-value pairs of arg_id and a value.
    """
    data = request.get_json()

    # add node
    try:
        node = Node(task_template_version_id=data['task_template_version_id'],
                    node_args_hash=data['node_args_hash'])
        DB.session.add(node)
        DB.session.commit()

        # lock for insert to related tables
        DB.session.refresh(node, with_for_update=True)

        # add node_args
        node_args = json.loads(data['node_args'])
        for arg_id, value in node_args.items():
            app.logger.info(f'Adding node_arg with node_id: {node.id}, '
                            f'arg_id: {arg_id}, and val: {value}')
            node_arg = NodeArg(node_id=node.id, arg_id=arg_id, val=value)
            DB.session.add(node_arg)
        DB.session.commit()
        # return result
        resp = jsonify(node_id=node.id)
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM node
            WHERE
                task_template_version_id = :task_template_version_id
                AND node_args_hash = :node_args_hash
        """
        node = DB.session.query(Node).from_statement(text(query)).params(
            task_template_version_id=data['task_template_version_id'],
            node_args_hash=data['node_args_hash']
        ).one()
        DB.session.commit()
        # return result
        resp = jsonify(node_id=node.id)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(sr(e), app.logger)




@jobmon_client.route('/dag', methods=['GET'])
def get_dag_id():
    """Get a dag id: If a matching dag isn't found, return None.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    try:
        query = """SELECT dag.id FROM dag WHERE hash = :dag_hash"""
        result = DB.session.query(Dag).from_statement(text(query)).params(
            dag_hash=request.args["dag_hash"]
        ).one_or_none()

        if result is None:
            resp = jsonify({'dag_id': None})
        else:
            resp = jsonify({'dag_id': result.id})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/dag', methods=['POST'])
def add_dag():
    """Add a new dag to the database.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    data = request.get_json()

    # add dag
    dag_hash = data.pop("dag_hash")
    nodes_and_edges = data.pop("nodes_and_edges")
    try:
        dag = Dag(hash=dag_hash)
        DB.session.add(dag)
        DB.session.commit()

        # now get a lock to add the edges
        DB.session.refresh(dag, with_for_update=True)

        edges_to_add = []

        for node_id, edges in nodes_and_edges.items():
            app.logger.debug(f'Edges: {edges}')

            if len(edges['upstream_nodes']) == 0:
                upstream_nodes = None
            else:
                upstream_nodes = str(edges['upstream_nodes'])

            if len(edges['downstream_nodes']) == 0:
                downstream_nodes = None
            else:
                downstream_nodes = str(edges['downstream_nodes'])

            edge = Edge(dag_id=dag.id,
                        node_id=node_id,
                        upstream_nodes=upstream_nodes,
                        downstream_nodes=downstream_nodes)
            edges_to_add.append(edge)

        DB.session.bulk_save_objects(edges_to_add)
        DB.session.commit()
        # return result
        resp = jsonify(dag_id=dag.id)
        resp.status_code = StatusCodes.OK

        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM dag
            WHERE hash = :dag_hash
        """
        dag = DB.session.query(Dag).from_statement(text(query)).params(dag_hash=dag_hash).one()
        DB.session.commit()
        # return result
        resp = jsonify(dag_id=dag.id)
        resp.status_code = StatusCodes.OK

        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)



@jobmon_client.route('/task', methods=['GET'])
def get_task_id_and_status():
    try:
        wid = request.args['workflow_id']
        int(wid)
        nid = request.args['node_id']
        int(nid)
        h = request.args["task_args_hash"]
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
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
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/task', methods=['POST'])
def add_task():
    """Add a job to the database

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
        logger.debug(data)
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
                    type_id = _add_or_get_attribute_type(name)
                    task_attribute = TaskAttribute(task_id=task.id,
                                                   attribute_type=type_id,
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
        raise_user_error(str(e), app.logger)
    except TypeError as e:
        raise_user_error(str(e), app.logger)
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/task/<task_id>/update_parameters', methods=['PUT'])
def update_task_parameters(task_id: int):
    data = request.get_json()
    try:
        int(task_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)

    try:
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
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/task/bind_tasks', methods=['PUT'])
def bind_tasks():
    try:
        all_data = request.get_json()
        logger.debug(all_data)
        tasks = all_data["tasks"]
        # receive from client the tasks in a format of:
        #{<hash>:[workflow_id(0), node_id(1), task_args_hash(2), name(3), command(4), max_attempts(5)], reset_if_running(6),
        # task_args(7),task_attributes(8)}
        to_add = {}
        to_update = {}
        for k in tasks.keys():
            query = """
                    SELECT task.id, task.status
                    FROM task
                    WHERE
                        workflow_id = :workflow_id
                        AND node_id = :node_id
                        AND task_args_hash = :task_args_hash
                """
            result = DB.session.query(Task).from_statement(text(query)).params(
                workflow_id=tasks[k][0],
                node_id=tasks[k][1],
                task_args_hash=tasks[k][2]
            ).one_or_none()

            if result is None:
                task = Task(
                    workflow_id=int(tasks[k][0]),
                    node_id=int(tasks[k][1]),
                    task_args_hash=tasks[k][2],
                    name=tasks[k][3],
                    command=tasks[k][4],
                    max_attempts=tasks[k][5],
                    status=TaskStatus.REGISTERED)
                to_add[k] = task
            else:
                query = """SELECT task.* FROM task WHERE task.id = :task_id"""
                task = DB.session.query(Task).from_statement(text(query)).params(
                    task_id=result.id).one()
                task.reset(name=tasks[k][3], command=tasks[k][4],
                           max_attempts=tasks[k][5],
                           reset_if_running=tasks[k][6])
                to_update[k] = task
        DB.session.add_all(to_add.values())
        DB.session.flush()
        DB.session.add_all(to_update.values())
        DB.session.flush()
        # continue add
        task_args = []
        task_attribute_list = []
        for k in to_add.keys():
            task = to_add[k]
            logger.debug(k)
            logger.debug(tasks[k])
            for _id in tasks[k][7].keys():
                task_arg = TaskArg(task_id=task.id, arg_id=int(_id), val=tasks[k][7][_id])
                task_args.append(task_arg)

            for name in tasks[k][8].keys():
                type_id = _add_or_get_attribute_type(name)
                task_attribute = TaskAttribute(task_id=task.id,
                                               attribute_type=type_id,
                                               value=tasks[k][8][name])
                task_attribute_list.append(task_attribute)

        DB.session.add_all(task_args)
        DB.session.flush()
        DB.session.add_all(task_attribute_list)
        DB.session.flush()
        # continue update
        inserts = []
        updates = []
        for k in to_update.keys():
            task = to_update[k]
            for name in tasks[k][8].keys():
                attribute_type = _add_or_get_attribute_type(name)
                insert_vals = insert(TaskAttribute).values(
                    task_id=task.id,
                    attribute_type=attribute_type,
                    value=tasks[k][8][name]
                )
                inserts.append(insert_vals)
                update_insert = insert_vals.on_duplicate_key_update(
                    value=insert_vals.inserted.value,
                    status='U'
                )
                updates.append(update_insert)
        DB.session.add_all(inserts)
        DB.session.flush()
        DB.session.add_all(updates)
        DB.session.flush()
        DB.session.commit()
        # return a dict of tasks {<hash>: [id, status]}
        return_tasks = {}
        for k in to_add.keys():
            return_tasks[k] = [to_add[k].id, to_add[k].status]
        for k in to_update.keys():
            return_tasks[k] = [to_update[k].id, to_update[k].status]
        resp = jsonify(tasks=return_tasks)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


def _add_or_get_attribute_type(name: str) -> int:
    try:
        attribute_type = TaskAttributeType(name=name)
        DB.session.add(attribute_type)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT id, name
            FROM task_attribute_type
            WHERE name = :name
        """
        attribute_type = DB.session.query(TaskAttributeType) \
            .from_statement(text(query)).params(name=name).one()

    return attribute_type.id


def _add_or_update_attribute(task_id: int, name: str, value: str) -> int:
    attribute_type = _add_or_get_attribute_type(name)
    insert_vals = insert(TaskAttribute).values(
        task_id=task_id,
        attribute_type=attribute_type,
        value=value
    )
    update_insert = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value,
        status='U'
    )
    attribute = DB.session.execute(update_insert)
    DB.session.commit()
    return attribute.id


@jobmon_client.route('/task/<task_id>/task_attributes', methods=['PUT'])
def update_task_attribute(task_id: int):
    """Add or update attributes for a task"""
    try:
        int(task_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        data = request.get_json()
        attributes = data["task_attributes"]
        # update existing attributes with their values
        for name, val in attributes.items():
            _add_or_update_attribute(task_id, name, val)
        # Flask requires that a response is returned, no values need to be passed back
        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow', methods=['GET'])
def get_workflow_id_and_status():
    try:
        dag_id = request.args['dag_id']
        int(dag_id)
        tv_id = request.args['tool_version_id']
        int(tv_id)
        whash = request.args['workflow_args_hash']
        int(whash)
        thash = request.args['task_hash']
        int(thash)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        query = """
            SELECT workflow.id, workflow.status
            FROM workflow
            WHERE
                tool_version_id = :tool_version_id
                AND dag_id = :dag_id
                AND workflow_args_hash = :workflow_args_hash
                AND task_hash = :task_hash
        """
        result = DB.session.query(Workflow).from_statement(text(query)).params(
            tool_version_id=tv_id,
            dag_id=dag_id,
            workflow_args_hash=whash,
            task_hash=thash
        ).one_or_none()

        # send back json
        if result is None:
            resp = jsonify({'workflow_id': None, 'status': None})
        else:
            resp = jsonify({'workflow_id': result.id,
                            'status': result.status})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow', methods=['POST'])
def add_workflow():
    """Add a workflow to the database."""
    data = request.get_json()
    try:
        tv_id = data['tool_version_id']
        int(tv_id)
        dag_id = data['dag_id']
        int(dag_id)
        whash = data['workflow_args_hash']
        int(whash)
        thash = data['task_hash']
        int(thash)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        workflow = Workflow(tool_version_id=tv_id,
                            dag_id=dag_id,
                            workflow_args_hash=whash,
                            task_hash=thash,
                            description=data['description'],
                            name=data["name"],
                            workflow_args=data["workflow_args"])
        DB.session.add(workflow)
        # TODO: doesn't work with flush, figure out why. Using commit breaks atomicity, workflow
        # attributes may not populate correctly on a rerun
        DB.session.commit()
        if data['workflow_attributes']:
            wf_attributes_list = []
            for name, val in data['workflow_attributes'].items():
                wf_type_id = _add_or_get_wf_attribute_type(name)
                wf_attribute = WorkflowAttribute(workflow_id=workflow.id,
                                                 workflow_attribute_type_id=wf_type_id,
                                                 value=val)
                wf_attributes_list.append(wf_attribute)
            DB.session.add_all(wf_attributes_list)
            DB.session.flush()
        DB.session.commit()

        resp = jsonify(workflow_id=workflow.id)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow/<workflow_args_hash>', methods=['GET'])
def get_matching_workflows_by_workflow_args(workflow_args_hash: int):
    """
    Return any dag hashes that are assigned to workflows with identical
    workflow args
    """
    try:
        int(workflow_args_hash)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        query = """
            SELECT workflow.task_hash, workflow.tool_version_id, dag.hash
            FROM workflow
            JOIN dag
                ON workflow.dag_id = dag.id
            WHERE
                workflow.workflow_args_hash = :workflow_args_hash
        """

        res = DB.session.query(Workflow.task_hash, Workflow.tool_version_id,
                               Dag.hash).from_statement(text(query)).params(
            workflow_args_hash=workflow_args_hash
        ).all()
        DB.session.commit()
        res = [(row.task_hash, row.tool_version_id, row.hash) for row in res]
        resp = jsonify(matching_workflows=res)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow_run/<workflow_run_id>/is_resumable', methods=['GET'])
def workflow_run_is_terminated(workflow_run_id: int):
    try:
        int(workflow_run_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        query = """
            SELECT
                workflow_run.*
            FROM
                workflow_run
            WHERE
                workflow_run.id = :workflow_run_id
                AND (
                    workflow_run.status = 'T'
                    OR workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
                )
        """
        res = DB.session.query(WorkflowRun).from_statement(text(query)).params(
            workflow_run_id=workflow_run_id
        ).one_or_none()
        DB.session.commit()

        if res is not None:
            # try to transition the workflow. Send back any competing
            # workflow_run_id and its status
            try:
                res.workflow.transition(WorkflowStatus.CREATED)
                DB.session.commit()
            except InvalidStateTransition:
                DB.session.rollback()
                raise

            resp = jsonify(workflow_run_status=res.status)
        else:
            resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


def _add_or_get_wf_attribute_type(name: str) -> int:
    try:
        wf_attrib_type = WorkflowAttributeType(name=name)
        DB.session.add(wf_attrib_type)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
        SELECT id, name
        FROM workflow_attribute_type
        WHERE name = :name
        """
        wf_attrib_type = DB.session.query(WorkflowAttributeType)\
            .from_statement(text(query)).params(name=name).one()

    return wf_attrib_type.id


def _upsert_wf_attribute(workflow_id: int, name: str, value: str):
    wf_attrib_id = _add_or_get_wf_attribute_type(name)
    insert_vals = insert(WorkflowAttribute).values(
        workflow_id=workflow_id,
        workflow_attribute_type_id=wf_attrib_id,
        value=value
        )

    upsert_stmt = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value,
        status='U')

    DB.session.execute(upsert_stmt)
    DB.session.commit()


@jobmon_client.route('/workflow/<workflow_id>/workflow_attributes', methods=['PUT'])
def update_workflow_attribute(workflow_id: int):
    try:
        int(workflow_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    """ Add/update attributes for a workflow """
    try:
        data = request.get_json()
        attributes = data["workflow_attributes"]
        if attributes:
            for name, val in attributes.items():
                _upsert_wf_attribute(workflow_id, name, val)

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow_run', methods=['POST'])
def add_workflow_run():
    try:
        data = request.get_json()
        wid = data["workflow_id"]
        int(wid)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        workflow_run = WorkflowRun(
            workflow_id=wid,
            user=data["user"],
            executor_class=data["executor_class"],
            jobmon_version=data["jobmon_version"],
            status=WorkflowRunStatus.REGISTERED)
        DB.session.add(workflow_run)
        DB.session.commit()

        # refresh in case of race condition
        workflow = workflow_run.workflow
        DB.session.refresh(workflow, with_for_update=True)
        # try to transition the workflow. Send back any competing workflow_run_id
        # and its status

        workflow.transition(WorkflowStatus.CREATED)
        DB.session.commit()
        previous_wfr = []
        resp = jsonify(workflow_run_id=workflow_run.id,
                       status=workflow_run.status,
                       previous_wfr=previous_wfr)
        resp.status_code = StatusCodes.OK
        return resp
    except InvalidStateTransition:
        created_states = [WorkflowStatus.CREATED]
        active_states = [WorkflowStatus.BOUND, WorkflowStatus.RUNNING]

        # a workflow is already in created state. thats a race condition. set
        # workflow run status to error. leave workflow alone
        if workflow.status in created_states:
            workflow_run.status = WorkflowRunStatus.ERROR
            DB.session.add(workflow_run)
            DB.session.commit()
            previous_wfr = [
                (wfr.id, wfr.status) for wfr in workflow.workflow_runs
                if wfr.status in created_states]

        # a workflow is running or about to start
        elif workflow.status in active_states:

            # if resume is set return the workflow that was set to hot or cold
            # resume
            if data["resume"]:
                resumed_wfr = workflow.resume(data["reset_running_jobs"])
                DB.session.commit()
                previous_wfr = [(wfr.id, wfr.status) for wfr in resumed_wfr]

            # otherwise return the workflow that is in an active state
            else:
                previous_wfr = [
                    (wfr.id, wfr.status) for wfr in workflow.workflow_runs
                    if wfr.status in active_states]

        else:
            app.logger.error("how did I get here? all other transitions are valid")
        resp = jsonify(workflow_run_id=workflow_run.id,
                       status=workflow_run.status,
                       previous_wfr=previous_wfr)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
def terminate_workflow_run(workflow_run_id: int):
    try:
        int(workflow_run_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        workflow_run = DB.session.query(WorkflowRun).filter_by(
            id=workflow_run_id).one()

        if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
            states = [TaskStatus.INSTANTIATED]
        elif workflow_run.status == WorkflowRunStatus.COLD_RESUME:
            states = [TaskStatus.INSTANTIATED, TaskInstanceStatus.RUNNING]

        # add error logs
        log_errors = """
            INSERT INTO task_instance_error_log
                (task_instance_id, description, error_time)
            SELECT
                task_instance.id,
                CONCAT(
                    'Workflow resume requested. Setting to K from status of: ',
                    task_instance.status
                ) as description,
                CURRENT_TIMESTAMP as error_time
            FROM task_instance
            JOIN task
                ON task_instance.task_id = task.id
            WHERE
                task_instance.workflow_run_id = :workflow_run_id
                AND task.status IN :states
        """
        DB.session.execute(log_errors,
                           {"workflow_run_id": int(workflow_run_id),
                            "states": states})
        DB.session.flush()

        # update job instance states
        update_task_instance = """
            UPDATE
                task_instance
            JOIN task
                ON task_instance.task_id = task.id
            SET
                task_instance.status = 'K',
                task_instance.status_date = CURRENT_TIMESTAMP
            WHERE
                task_instance.workflow_run_id = :workflow_run_id
                AND task.status IN :states
        """
        DB.session.execute(update_task_instance,
                           {"workflow_run_id": workflow_run_id,
                            "states": states})
        DB.session.flush()

        # transition to terminated
        workflow_run.transition(WorkflowRunStatus.TERMINATED)
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow_run/<workflow_run_id>/delete', methods=['PUT'])
def delete_workflow_run(workflow_run_id: int):
    try:
        int(workflow_run_id)
    except Exception as e:
        raise_user_error(str(e), app.logger)
    try:
        query = "DELETE FROM workflow_run where workflow_run.id = :workflow_run_id"
        DB.session.execute(query,
                           {"workflow_run_id": workflow_run_id})
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)


@jobmon_client.route('/workflow_run_status', methods=['GET'])
def get_active_workflow_runs() -> Dict:
    """Return all workflow runs that are currently in the specified state."""
    # logger.info(logging.myself())
    try:
        query = """
            SELECT
                workflow_run.*
            FROM
                workflow_run
            WHERE
                workflow_run.status in :workflow_run_status
        """
        workflow_runs = DB.session.query(WorkflowRun).from_statement(text(query)).params(
            workflow_run_status=request.args.getlist('status')
        ).all()
        DB.session.commit()
        workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
        resp = jsonify(workflow_runs=workflow_runs)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        log_and_raise(str(e), app.logger)