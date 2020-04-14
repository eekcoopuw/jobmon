from flask import jsonify, request, Blueprint
from http import HTTPStatus as StatusCodes
import json
import os
import socket
from sqlalchemy.sql import func, text
import sqlalchemy
import traceback
from typing import Optional, Any

from jobmon import config
from jobmon.models import DB
from jobmon.models.arg import Arg
from jobmon.models.arg_type import ArgType
from jobmon.models.attributes.constants import qsub_attribute, task_instance_attribute
from jobmon.models.attributes.task_attribute import TaskAttribute
from jobmon.models.attributes.task_attribute_type import TaskAttributeType
from jobmon.models.attributes.task_instance_attribute import TaskInstanceAttribute
from jobmon.models.command_template_arg_type_mapping import \
    CommandTemplateArgTypeMapping
from jobmon.models.dag import Dag
from jobmon.models.edge import Edge
from jobmon.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
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
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.server.server_side_exception import log_and_raise

jsm = Blueprint("job_state_manager", __name__)


# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)


@jsm.errorhandler(404)
def page_not_found(error):
    return 'This route does not exist {}'.format(request.url), 404


def get_time(session):
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


@jsm.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening
    """
    logger.info(logging.myself())
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


# ############################## CLIENT ROUTES ################################
@jsm.route('/tool', methods=['POST'])
def add_tool():
    """Add a tool to the database"""
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    try:
        tool = Tool(name=data["name"])
        DB.session.add(tool)
        DB.session.commit()
        tool = tool.to_wire_as_client_tool()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        tool = None
    resp = jsonify(tool=tool)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/tool_version', methods=['POST'])
def add_tool_version():
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    tool_version = ToolVersion(
        tool_id=data["tool_id"])
    DB.session.add(tool_version)
    DB.session.commit()
    tool_version = tool_version.to_wire_as_client_tool_version()
    resp = jsonify(tool_version=tool_version)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_template', methods=['POST'])
def add_task_template():
    """Add a tool to the database"""
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    tt = TaskTemplate(tool_version_id=data["tool_version_id"],
                      name=data["name"])
    DB.session.add(tt)
    DB.session.commit()
    resp = jsonify(task_template_id=tt.id)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_arg(name: str) -> Arg:
    try:
        query = """
        SELECT id, name
        FROM arg
        WHERE name = :name
        """
        arg = DB.session.query(Arg).from_statement(text(query))\
            .params(name=name).one()
    except sqlalchemy.orm.exc.NoResultFound:
        DB.session.rollback()
        arg = Arg(name=name)
        DB.session.add(arg)
        DB.session.commit()
    return arg


@jsm.route('/task_template/<task_template_id>/add_version', methods=['POST'])
def add_task_template_version(task_template_id: int):
    """Add a tool to the database"""
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    # create task template version if we didn't find a match
    arg_mapping_dct: dict = {ArgType.NODE_ARG: [],
                             ArgType.TASK_ARG: [],
                             ArgType.OP_ARG: []}
    for arg_name in data["node_args"]:
        arg_mapping_dct[ArgType.NODE_ARG].append(_add_or_get_arg(arg_name))
    for arg_name in data["task_args"]:
        arg_mapping_dct[ArgType.TASK_ARG].append(_add_or_get_arg(arg_name))
    for arg_name in data["op_args"]:
        arg_mapping_dct[ArgType.OP_ARG].append(_add_or_get_arg(arg_name))
    ttv = TaskTemplateVersion(task_template_id=task_template_id,
                              command_template=data["command_template"],
                              arg_mapping_hash=data["arg_mapping_hash"])
    DB.session.add(ttv)
    DB.session.flush()
    for arg_type_id in arg_mapping_dct.keys():
        for arg in arg_mapping_dct[arg_type_id]:
            ctatm = CommandTemplateArgTypeMapping(
                task_template_version_id=ttv.id,
                arg_id=arg.id,
                arg_type_id=arg_type_id)
            DB.session.add(ctatm)
    DB.session.commit()

    resp = jsonify(
        task_template_version=ttv.to_wire_as_client_task_template_version())
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/node', methods=['POST'])
def add_node():
    """Add a new node to the database.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a
                        node.
        task_template_version_id: version id of the task_template a node
                                  belongs to.
        node_args: key-value pairs of arg_id and a value.
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    # add node
    node = Node(task_template_version_id=data['task_template_version_id'],
                node_args_hash=data['node_args_hash'])
    DB.session.add(node)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()

    # add node_args
    node_args = json.loads(data['node_args'])
    for arg_id, value in node_args.items():
        logger.info(f'Adding node_arg with node_id: {node.id}, '
                    f'arg_id: {arg_id}, and val: {value}')
        node_arg = NodeArg(node_id=node.id, arg_id=arg_id, val=value)
        DB.session.add(node_arg)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()

    # return result
    resp = jsonify(node_id=node.id)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/dag', methods=['POST'])
def add_dag():
    """Add a new dag to the database.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    # add dag
    dag = Dag(hash=data["dag_hash"])
    DB.session.add(dag)
    DB.session.commit()

    # return result
    resp = jsonify(dag_id=dag.id)
    resp.status_code = StatusCodes.OK

    return resp


@jsm.route('/edge/<dag_id>', methods=['POST'])
def add_edges(dag_id: int):
    """Add a group of edges to the database.

    Args:
        dag_id: identifies the dag whose edges are being inserted
        nodes_and_edges: a json object with the following format:
            {jobmon/server/deployment/container/db/table015-workflow.sql
                node_id: {
                    'upstream_nodes': [node_id, node_id, node_id],
                    'downstream_nodes': [node_id, node_id]
                },
                node_id: {...},
                ...
            }
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(f'Data received to add_edges: {data} with type: {type(data)}')

    for node_id, edges in data.items():
        logger.debug(f'Edges: {edges}')

        if len(edges['upstream_nodes']) == 0:
            upstream_nodes = None
        else:
            upstream_nodes = str(edges['upstream_nodes'])

        if len(edges['downstream_nodes']) == 0:
            downstream_nodes = None
        else:
            downstream_nodes = str(edges['downstream_nodes'])

        edge = Edge(dag_id=dag_id,
                    node_id=node_id,
                    upstream_nodes=upstream_nodes,
                    downstream_nodes=downstream_nodes)
        DB.session.add(edge)
        DB.session.commit()

    return '', StatusCodes.OK


@jsm.route('/task', methods=['POST'])
def add_task():
    """Add a job to the database

    Args:
        workflow_id: workflow this task is associated with
        node_id: structural node this task is associated with
        task_arg_hash: hash of the data args for this task
        name: task's name
        command: task's command
        max_attempts: how many times the job should be attempted
        task_args: dictionary of data args for this task
        task_attributes: dictionary of attributes associated with the task
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    task = Task(
        workflow_id=data["workflow_id"],
        node_id=data["node_id"],
        task_args_hash=data["task_args_hash"],
        name=data["name"],
        command=data["command"],
        max_attempts=data["max_attempts"],
        status=TaskStatus.REGISTERED)
    DB.session.add(task)
    DB.session.flush()
    for _id, val in data["task_args"].items():
        task_arg = TaskArg(task_id=task.id, arg_id=_id, val=val)
        DB.session.add(task_arg)
        DB.session.flush()
    for name, val in data["task_attributes"].items():
        type_id = _add_or_get_attribute_type(name)
        task_attribute = TaskAttribute(task_id=task.id,
                                       attribute_type=type_id,
                                       value=val)
        DB.session.add(task_attribute)
        DB.session.flush()
    DB.session.commit()

    resp = jsonify(task_id=task.id)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task/<task_id>/update_parameters', methods=['PUT'])
def update_task_parameters(task_id: int):
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

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


def _add_or_get_attribute_type(name: str) -> int:
    try:
        query = """
        SELECT id, name
        FROM task_attribute_type
        WHERE name = :name
        """
        attribute_type = DB.session.query(TaskAttributeType)\
            .from_statement(text(query)).params(name=name).one()
    except sqlalchemy.orm.exc.NoResultFound:
        DB.session.rollback()
        attribute_type = TaskAttributeType(name=name)
        DB.session.add(attribute_type)
        DB.session.commit()
    return attribute_type.id


def _add_or_update_attribute(task_id: int, name: str, value: str) -> int:
    attribute_type = _add_or_get_attribute_type(name)
    try:
        # if the attribute was already set for the task, update it with the
        # new value
        query = """
        SELECT id
        FROM task_attribute
        WHERE
            task_id = :task_id
            AND attribute_type = :attribute_id
        """
        attr = DB.session.query(TaskAttribute).from_statement(text(query))\
            .params(task_id=task_id, attribute_type=attribute_type).one()
        params = {"value": value, "attribute_id": attr.id}
        update_query = """
            UPDATE task_attribute
            SET value = :value
            WHERE attribute_id = :attribute_id"""
        DB.session.execute(update_query, params)
    except sqlalchemy.orm.exc.NoResultFound:
        DB.session.rollback()
        attr = TaskAttribute(task_id=task_id,
                             attribute_type=attribute_type,
                             value=value)
        DB.session.add(attr)
        DB.session.commit()
    return attr.id


@jsm.route('/task/<task_id>/task_attributes', methods=['PUT'])
def update_task_attribute(task_id: int):
    """Add or update attributes for a task"""
    data = request.get_json()
    attributes = data["task_attributes"]
    # update existing attributes with their values
    for name, val in attributes:
        _add_or_update_attribute(task_id, name, val)


@jsm.route('/workflow', methods=['POST'])
def add_workflow():
    """Add a workflow to the database or update it (via PUT)"""
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    workflow = Workflow(tool_version_id=data['tool_version_id'],
                        dag_id=data['dag_id'],
                        workflow_args_hash=data['workflow_args_hash'],
                        task_hash=data['task_hash'],
                        description=data['description'],
                        name=data["name"],
                        workflow_args=data["workflow_args"])
    DB.session.add(workflow)
    DB.session.commit()

    resp = jsonify(workflow_id=workflow.id)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run', methods=['POST'])
def add_workflow_run():
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)

    workflow_run = WorkflowRun(
        workflow_id=data["workflow_id"],
        user=data["user"],
        executor_class=data["executor_class"],
        jobmon_version=data["jobmon_version"],
        status=WorkflowRunStatus.REGISTERED)
    DB.session.add(workflow_run)
    DB.session.commit()

    # refresh in case of race condition
    workflow = workflow_run.workflow
    DB.session.refresh(workflow)  # TODO: consider refreshing with write lock

    # try to transition the workflow. Send back any competing workflow_run_id
    # and its status
    try:
        workflow.transition(WorkflowStatus.CREATED)
        DB.session.commit()
        previous_wfr = []

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
            logger.error("how did I get here? all other transitions are valid")

    resp = jsonify(workflow_run_id=workflow_run.id,
                   status=workflow_run.status,
                   previous_wfr=previous_wfr)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
def terminate_workflow_run(workflow_run_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))

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
            UTC_TIMESTAMP as error_time
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
            task_instance.status_date = UTC_TIMESTAMP
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


@jsm.route('/workflow_run/<workflow_run_id>/delete', methods=['PUT'])
def delete_workflow_run(workflow_run_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))

    query = "DELETE FROM workflow_run where workflow_run.id = :workflow_run_id"
    DB.session.execute(query,
                       {"workflow_run_id": workflow_run_id})
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


# ############################ SCHEDULER ROUTES ###############################
@jsm.route('/workflow_run/<workflow_run_id>/log_executor_report_by',
           methods=['POST'])
def log_executor_report_by(workflow_run_id: int):
    logger.info(logging.myself())
    data = request.get_json()

    params = {"workflow_run_id": int(workflow_run_id)}
    for key in ["next_report_increment", "executor_ids"]:
        params[key] = data[key]

    if params["executor_ids"]:
        query = """
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE
                workflow_run_id = :workflow_run_id
                AND executor_id in :executor_ids"""
        DB.session.execute(query, params)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance', methods=['POST'])
def add_task_instance():
    """Add a task_instance to the database

    Args:
        task_id (int): unique id for the task
        executor_type (str): string name of the executor type used
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    logger.debug(f"Add TI for task {data['task_id']}")

    # query task
    task = DB.session.query(Task).filter_by(id=data['task_id']).first()
    DB.session.commit()

    # create task_instance from task parameters
    task_instance = TaskInstance(
        workflow_run_id=data["workflow_run_id"],
        executor_type=data['executor_type'],
        task_id=data['task_id'],
        executor_parameter_set_id=task.executor_parameter_set_id)
    DB.session.add(task_instance)
    DB.session.commit()

    try:
        task_instance.task.transition(TaskStatus.INSTANTIATED)
    except InvalidStateTransition:
        # TODO: what race condition is this covering?
        if task_instance.job.status == TaskStatus.INSTANTIATED:
            msg = ("Caught InvalidStateTransition. Not transitioning task "
                   "{}'s task_instance_id {} from I to I"
                   .format(data['task_id'], task_instance.id))
            logger.warning(msg)
        else:
            raise
    finally:
        DB.session.commit()
    resp = jsonify(
        task_instance=task_instance.to_wire_as_executor_task_instance())
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_no_executor_id',
           methods=['POST'])
def log_no_executor_id(task_instance_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    logger.debug(f"Log NO EXECUTOR ID for TI {task_instance_id}."
                 f"Data {data['executor_id']}")
    logger.debug(f"Add TI for task ")

    if data['executor_id'] == qsub_attribute.NO_EXEC_ID:
        logger.info("Qsub was unsuccessful and caused an exception")
    else:
        logger.info("Qsub may have run, but the sge job id could not be parsed"
                    " from the qsub response so no executor id can be assigned"
                    " at this time")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(ti, TaskInstanceStatus.NO_EXECUTOR_ID)
    ti.executor_id = data['executor_id']
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_executor_id',
           methods=['POST'])
def log_executor_id(task_instance_id: int):
    """Log a task_instance's executor id
    Args:

        task_instance_id: id of the task_instance to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    logger.debug(f"Log EXECUTOR ID for TI {task_instance_id}."
                 f"Data {data}")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(
        ti, TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    ti.executor_id = data['executor_id']
    ti.report_by_date = func.ADDTIME(
        func.UTC_TIMESTAMP(),
        func.SEC_TO_TIME(data["next_report_increment"]))
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_error_reconciler',
           methods=['POST'])
def log_error_reconciler(task_instance_id: int):
    """Log a task_instance as errored
    Args:
        task_instance_id (int): id for task instance
        data:
        oom_killed: whether or not given job errored due to an oom-kill event
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    error_state = data['error_state']
    error_message = data['error_message']
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    logger.debug(f"Log ERROR for TI:{task_instance_id}. Data: {data}")

    query = """
        SELECT
            task_instance.*
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
            AND task_instance.report_by_date <= UTC_TIMESTAMP()
    """
    ti = DB.session.query(TaskInstance).from_statement(text(query)).params(
        task_instance_id=task_instance_id).one_or_none()

    # make sure the task hasn't logged a new heartbeat since we began
    # reconciliation
    if ti is not None:
        try:
            resp = _log_error(ti, error_state, error_message, executor_id,
                              nodename)
        except sqlalchemy.exc.OperationalError:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(ti, error_state, new_msg, executor_id, nodename)
    else:
        resp = jsonify()
        resp.status_code = StatusCodes.OK

    return resp


@jsm.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def log_workflow_run_heartbeat(workflow_run_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))
    data = request.get_json()
    logger.debug(f"Heartbeat data: {data}")

    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()

    try:
        workflow_run.heartbeat(data["next_report_increment"])
        DB.session.commit()
        logger.debug(f"wfr {workflow_run_id} heartbeat confirmed")
    except InvalidStateTransition:
        DB.session.rollback()
        logger.debug(f"wfr {workflow_run_id} heartbeat rolled back")

    resp = jsonify(message=str(workflow_run.status))
    resp.status_code = StatusCodes.OK
    return resp


# ############################## SWARM ROUTES #################################

@jsm.route('/task/<task_id>/queue', methods=['POST'])
def queue_job(task_id: int):
    """Queue a job and change its status
    Args:

        job_id: id of the job to queue
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_id", task_id))

    task = DB.session.query(Task).filter_by(id=task_id).one()
    try:
        task.transition(TaskStatus.QUEUED_FOR_INSTANTIATION)
    except InvalidStateTransition:
        # TODO: what race condition is this covering?
        if task.status == TaskStatus.QUEUED_FOR_INSTANTIATION:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   f"{task_id} from Q to Q")
            logger.warning(msg)
        else:
            raise
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run/<workflow_run_id>/update_status', methods=['PUT'])
def log_workflow_run_status_update(workflow_run_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))
    data = request.get_json()
    logger.debug(f"Log status update for workflow_run_id:{workflow_run_id}."
                 f"Data: {data}")

    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()
    workflow_run.transition(data["status"])
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


# TODO: currently unused pending review of where it should go
@jsm.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def log_wfr_heartbeat(workflow_run_id: int):
    """Log a workflow_run as being responsive, with a heartbeat
    Args:

        workflow_run_id: id of the workflow_run to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))

    params = {"workflow_run_id": int(workflow_run_id)}
    query = """
        UPDATE workflow_run
        SET heartbeat_date = UTC_TIMESTAMP()
        WHERE id = :workflow_run_id"""
    DB.session.execute(query, params)
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


@jsm.route('/task/<task_id>/update_resources', methods=['POST'])
def update_task_resources(task_id: int):
    """ Change the resources set for a given task

    Args:
        task_id (int): id of the task for which resources will be changed
        parameter_set_type (str): parameter set type for this task
        max_runtime_seconds (int, optional): amount of time task is allowed to
            run for
        context_args (dict, optional): unstructured parameters to pass to
            executor
        queue (str, optional): sge queue to submit tasks to
        num_cores (int, optional): how many cores to get from sge
        m_mem_free ():
        j_resource (bool, optional): whether to request access to the j drive
        resource_scales (dict): values to scale by upon resource error
        hard_limit (bool): whether to move queues if requester resources exceed
            queue limits
    """

    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_id", task_id))

    data = request.get_json()
    parameter_set_type = data["parameter_set_type"]

    try:
        task_id = int(task_id)
    except ValueError:
        resp = jsonify(msg="task_id {} is not a number".format(task_id))
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp

    exec_params = ExecutorParameterSet(
        task_id=task_id,
        parameter_set_type=parameter_set_type,
        max_runtime_seconds=data.get('max_runtime_seconds', None),
        context_args=data.get('context_args', None),
        queue=data.get('queue', None),
        num_cores=data.get('num_cores', None),
        m_mem_free=_transform_mem_to_gb(data.get("m_mem_free")),
        j_resource=data.get('j_resource', False),
        resource_scales=data.get('resource_scales', None),
        hard_limits=data.get('hard_limits', False))
    DB.session.add(exec_params)
    DB.session.flush()  # get auto increment
    exec_params.activate()
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow/<workflow_id>/suspend', methods=['POST'])
def suspend_workflow(workflow_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_id", workflow_id))
    query = """
        UPDATE workflow
        SET status = "S"
        WHERE workflow.id = :workflow_id
    """
    DB.session.execute(query, {"workflow_id": workflow_id})
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp

# ############################## WORKER ROUTES ################################

@jsm.route('/task_instance/<task_instance_id>/log_running', methods=['POST'])
def log_running(task_instance_id: int):
    """Log a task_instance as running
    Args:

        task_instance_id: id of the task_instance to log as running
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    logger.debug(f"Log RUNNING for TI {task_instance_id}. Data={data}")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    msg = _update_task_instance_state(ti, TaskInstanceStatus.RUNNING)
    if data.get('executor_id', None) is not None:
        ti.executor_id = data['executor_id']
    if data.get('nodename', None) is not None:
        ti.nodename = data['nodename']
    ti.process_group_id = data['process_group_id']
    ti.report_by_date = func.ADDTIME(
        func.UTC_TIMESTAMP(), func.SEC_TO_TIME(data['next_report_increment']))
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_report_by', methods=['POST'])
def log_ti_report_by(task_instance_id: int):
    """Log a task_instance as being responsive with a new report_by_date, this
    is done at the worker node heartbeat_interval rate, so it may not happen at
    the same rate that the reconciler updates batch submitted report_by_dates
    (also because it causes a lot of traffic if all workers are logging report
    _by_dates often compared to if the reconciler runs often)
    Args:

        task_instance_id: id of the task_instance to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    logger.debug(f"Log report_by for TI {task_instance_id}. Data={data}")

    executor_id = data.get('executor_id', None)
    params = {}
    params["next_report_increment"] = data["next_report_increment"]
    params["task_instance_id"] = task_instance_id
    if executor_id is not None:
        params["executor_id"] = executor_id
        query = """
                UPDATE task_instance
                SET report_by_date = ADDTIME(
                    UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment)),
                    executor_id = :executor_id
                WHERE task_instance.id = :task_instance_id"""
    else:
        query = """
            UPDATE task_instance
            SET report_by_date = ADDTIME(
                UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE task_instance.id = :task_instance_id"""
    DB.session.execute(query, params)
    DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_usage', methods=['POST'])
def log_usage(task_instance_id: int):
    """Log the usage stats of a task_instance
    Args:

        task_instance_id: id of the task_instance to log done
        usage_str (str, optional): stats such as maxrss, etc
        wallclock (str, optional): wallclock of running job
        maxrss (str, optional): max resident set size mem used
        maxpss (str, optional): max proportional set size mem used
        cpu (str, optional): cpu used
        io (str, optional): io used
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    if data.get('maxrss', None) is None:
        data['maxrss'] = '-1'

    keys_to_attrs = {data.get('usage_str', None):
                     task_instance_attribute.USAGE_STR,
                     data.get('wallclock', None):
                         task_instance_attribute.WALLCLOCK,
                     data.get('cpu', None): task_instance_attribute.CPU,
                     data.get('io', None): task_instance_attribute.IO,
                     data.get('maxrss', None): task_instance_attribute.MAXRSS,
                     data.get('maxpss', None): task_instance_attribute.MAXPSS}

    logger.debug("usage_str is {}, wallclock is {}, maxrss is {}, "
                 "maxpss is {}, cpu is {}, io is {}"
                 .format(data.get('usage_str', None),
                         data.get('wallclock', None),
                         data.get('maxrss', None),
                         data.get('maxpss', None),
                         data.get('cpu', None),
                         data.get('io', None)))

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    if data.get('usage_str', None) is not None:
        ti.usage_str = data['usage_str']
    if data.get('wallclock', None) is not None:
        ti.wallclock = data['wallclock']
    if data.get('maxrss', None) is not None:
        ti.maxrss = data['maxrss']
    if data.get('maxpss', None) is not None:
        ti.maxpss = data['maxpss']
    if data.get('cpu', None) is not None:
        ti.cpu = data['cpu']
    if data.get('io', None) is not None:
        ti.io = data['io']
    DB.session.commit()

    # TODO: figure out if we want this
    # for k in keys_to_attrs:
    #     logger.debug(
    #         'The value of {kval} being set in the attribute table is {k}'.
    #         format(kval=keys_to_attrs[k], k=k))
    #     if k is not None:
    #         ta = (TaskInstanceAttribute(
    #               task_id=task_id, attribute_type=keys_to_attrs[k], value=k))
    #         DB.session.add(ta)
    #     else:
    #         logger.debug('The value has not been set, nothing to upload')
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_done', methods=['POST'])
def log_done(task_instance_id: int):
    """Log a task_instance as done

    Args:
        task_instance_id: id of the task_instance to log done
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    logger.debug(f"Log DONE for TI {task_instance_id}. Data: {data}")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    if data.get('executor_id', None) is not None:
        ti.executor_id = data['executor_id']
    if data.get('nodename', None) is not None:
        ti.nodename = data['nodename']
    msg = _update_task_instance_state(ti, TaskInstanceStatus.DONE)
    DB.session.commit()

    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<task_instance_id>/log_error_worker_node',
           methods=['POST'])
def log_error_worker_node(task_instance_id: int):
    """Log a task_instance as errored
    Args:

        task_instance_id (str): id of the task_instance to log done
        error_message (str): message to log as error
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("task_instance_id", task_instance_id))
    data = request.get_json()
    error_state = data['error_state']
    error_message = data['error_message']
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    logger.debug(f"Log ERROR for TI:{task_instance_id}. Data: {data}")

    ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    try:
        resp = _log_error(ti, error_state, error_message, executor_id,
                          nodename)
    except sqlalchemy.exc.OperationalError:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ti, error_state, new_msg, executor_id, nodename)

    return resp


# ############################ HELPER FUNCTIONS ###############################
def _update_task_instance_state(task_instance: TaskInstance, status_id: str):
    """Advance the states of task_instance and it's associated Task,
    return any messages that should be published based on
    the transition

    Args:
        task_instance (obj) object of time models.TaskInstance
        status_id (int): id of the status to which to transition
    """
    logger.info(logging.myself())
    logger.debug(f"Update TI state {status_id} for {task_instance}")
    response = ""
    try:
        task_instance.transition(status_id)
    except InvalidStateTransition:
        if task_instance.status == status_id:
            # It was already in that state, just log it
            msg = f"Attempting to transition to existing state." \
                f"Not transitioning task, tid= " \
                f"{task_instance.id} from {task_instance.status} to " \
                f"{status_id}"
            logger.warning(msg)
        else:
            # Tried to move to an illegal state
            msg = f"Illegal state transition. Not transitioning task, " \
                f"tid={task_instance.id}, from {task_instance.status} to " \
                f"{status_id}"
            logger.error(msg)
    except KillSelfTransition:
        msg = f"kill self, cannot transition " \
              f"tid={task_instance.id}"
        logger.warning(msg)
        response = "kill self"
    except Exception as e:
        msg = f"General exception in _update_task_instance_state, " \
            f"jid {task_instance}, transitioning to {task_instance}. " \
            f"Not transitioning task. {e}"
        log_and_raise(msg, logger)

    return response


def _log_error(ti: TaskInstance, error_state: int, error_msg: str,
               executor_id: Optional[int] = None,
               nodename: Optional[str] = None):
    logger.info(logging.myself())
    if nodename is not None:
        ti.nodename = nodename
    if executor_id is not None:
        ti.executor_id = executor_id

    try:
        error = TaskInstanceErrorLog(task_instance_id=ti.id,
                                     description=error_msg)
        DB.session.add(error)
        msg = _update_task_instance_state(ti, error_state)
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        DB.session.rollback()
        logger.warning(str(e))
        raise

    return resp


@jsm.route('/log_level', methods=['GET'])
def get_log_level():
    """A simple 'action' to get the current server log level
    """
    logger.info(logging.myself())
    level: str = logging.getLevelName()
    logger.debug(level)
    resp = jsonify({'level': level})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/log_level/<level>', methods=['POST'])
def set_log_level(level: str):
    """Change log level
    Args:

        level: name of the log level. Takes CRITICAL, ERROR, WARNING, INFO,
            DEBUG

        data:
             loggers: a list of logger
                      Currently only support 'jobmonServer' and 'flask';
                      Other values will be ignored;
                      Empty list default to 'jobmonServer'.
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("level", level))
    level = level.upper()
    lev: int = logging.NOTSET

    if level == "CRITICAL":
        lev = logging.CRITICAL
    elif level == "ERROR":
        lev = logging.ERROR
    elif level == "WARNING":
        lev = logging.WARNING
    elif level == "INFO":
        lev = logging.INFO
    elif level == "DEBUG":
        lev = logging.DEBUG

    data = request.get_json()
    logger.debug(data)

    logger_list = []
    try:
        logger_list = data['loggers']
    except Exception:
        # Deliberately eat the exception. If no data provided, change all other
        # loggers except sqlalchemy
        pass

    if len(logger_list) == 0:
        # Default to reset jobmonServer log level
        logging.setlogLevel(lev)
    else:
        if 'jobmonServer' in logger_list:
            logging.setlogLevel(lev)
        elif 'flask' in logger_list:
            logging.setFlaskLogLevel(lev)

    resp = jsonify(msn="Set {loggers} server log to {level}".format(
        level=level, loggers=logger_list))
    resp.status_code = StatusCodes.OK
    return resp


def getLogLevelUseName(name: str) -> int:
    logger.info(logging.myself())
    logger.debug(logging.logParameter("name", name))
    log_level_dict = {"CRITICAL": logging.CRITICAL,
                      "ERROR": logging.ERROR,
                      "WARNING": logging.WARNING,
                      "INFO": logging.INFO,
                      "DEBUG": logging.DEBUG,
                      "NOTSET": logging.NOTSET}
    level = name.upper()
    if level not in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"):
        level = "NOTSET"
    return log_level_dict[level]


@jsm.route('/attach_remote_syslog/<level>/<host>/<port>/<sockettype>',
           methods=['POST'])
def attach_remote_syslog(level: str, host: str, port: int, sockettype: str):
    """
    Add a remote syslog handler

    :param level: remote syslog level
    :param host: remote syslog server host
    :param port: remote syslog server port
    :param port: remote syslog server socket type; unless specified as TCP,
    otherwise, UDP
    :return:
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("level", level))
    logger.debug(logging.logParameter("host", host))
    logger.debug(logging.logParameter("port", port))
    logger.debug(logging.logParameter("sockettype", sockettype))
    level = level.upper()
    if level not in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"):
        level = "NOTSET"

    try:
        port = int(port)
    except Exception:
        resp = jsonify(msn="Unable to convert {} to integer".format(port))
        resp.status_code = StatusCodes.BAD_REQUEST
        return resp

    s = socket.SOCK_DGRAM
    if sockettype.upper == "TCP":
        s = socket.SOCK_STREAM

    try:
        logging.attachSyslog(host=host, port=port, socktype=s,
                             l=getLogLevelUseName(level))
        resp = jsonify(msn="Attach syslog {h}:{p}".format(h=host, p=port))
        resp.status_code = StatusCodes.OK
        return resp
    except Exception:
        resp = jsonify(msn=traceback.format_exc())
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp


@jsm.route('/syslog_status', methods=['GET'])
def syslog_status():
    logger.info(logging.myself())
    if logging.isSyslogAttached():
        resp = jsonify({'syslog': True},
                       {'host': config.rsyslog_host},
                       {'port': config.rsyslog_port},
                       {'protocol': config.rsyslog_protocol})
        return resp
    resp = jsonify({'syslog': False})
    return resp


@jsm.route('/log_level_flask', methods=['GET'])
def get_log_level_flask():
    """A simple 'action' to get the current server log level
    """
    logger.info(logging.myself())
    level: str = logging.getFlaskLevelName()
    logger.debug(level)
    resp = jsonify({'level': level})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/log_level_flask/<level>', methods=['POST'])
def set_log_level_flask(level: str):
    """Change log level
    Args:

        level: name of the log level. Takes CRITICAL, ERROR, WARNING, INFO,
            DEBUG

        data:
             loggers: a list of logger
                      Currently only support 'jobmonServer' and 'flask';
                      Other values will be ignored;
                      Empty list default to 'jobmonServer'.
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("level", level))
    level = level.upper()
    lev: int = logging.NOTSET

    if level == "CRITICAL":
        lev = logging.CRITICAL
    elif level == "ERROR":
        lev = logging.ERROR
    elif level == "WARNING":
        lev = logging.WARNING
    elif level == "INFO":
        lev = logging.INFO
    elif level == "DEBUG":
        lev = logging.DEBUG

    data = request.get_json()
    logger.debug(data)

    logger_list = []
    try:
        logger_list = data['loggers']
    except Exception:
        # Deliberately eat the exception. If no data provided, change all other
        # loggers except sqlalchemy
        pass

    if len(logger_list) == 0:
        # Default to reset jobmonServer log level
        logging.setFlaskLogLevel(lev)
    else:
        if 'jobmonServer' in logger_list:
            logging.setlogLevel(lev)
        elif 'flask' in logger_list:
            logging.setFlaskLogLevel(lev)

    resp = jsonify(msn="Set {loggers} server log to {level}".format(
        level=level, loggers=logger_list))
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_instance/<executor_id>/maxpss/<maxpss>', methods=['POST'])
def set_maxpss(executor_id: int, maxpss: int):
    """
    Route to set maxpss of a job instance
    :param executor_id: sge execution id
    :return:
    """
    logger.debug(logging.myself())
    logging.logParameter("executor_id", executor_id)
    logging.logParameter("maxpss", maxpss)

    try:
        sql = f"UPDATE task_instance SET maxpss={maxpss} WHERE executor_id={executor_id}"
        DB.session.execute(sql)
        DB.session.commit()
        resp = jsonify(message=None)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        msg = "Error updating maxpss for execution id {eid}: {error}".format(eid=executor_id, error=str(e))
        logger.error(msg)
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
    finally:
        return resp
