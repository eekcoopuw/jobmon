from http import HTTPStatus as StatusCodes
import os

from datetime import datetime, timedelta
from flask import jsonify, request, Blueprint
from sqlalchemy.sql import text
from typing import Dict

from jobmon.models import DB
from jobmon.models.dag import Dag
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.node import Node
from jobmon.models.task import Task
from jobmon.models.task_instance import TaskInstance
from jobmon.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.models.task_instance_status import TaskInstanceStatus
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


# TODO: rename?
jqs = Blueprint("query_server", __name__)

logger = logging.getLogger(__name__)


@jqs.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening
    """
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route("/time", methods=['GET'])
def get_utc_now():
    time = DB.session.execute("SELECT UTC_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/tool/<tool_name>', methods=['GET'])
def get_tool(tool_name: str):
    logger.info(logging.myself())
    logging.logParameter("tool_name", tool_name)
    query = """
        SELECT
            tool.*
        FROM
            tool
        WHERE
            name = :tool_name"""
    tool = DB.session.query(Tool).from_statement(
        text(query)).params(tool_name=tool_name).one_or_none()
    DB.session.commit()
    if tool:
        tool = tool.to_wire_as_client_tool()
    resp = jsonify(tool=tool)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/tool/<tool_id>/tool_versions', methods=['GET'])
def get_tool_versions(tool_id: int):
    logger.info(logging.myself())
    logging.logParameter("tool_id", tool_id)
    query = """
        SELECT
            tool_version.*
        FROM
            tool_version
        WHERE
            tool_id = :tool_id"""
    tool_versions = DB.session.query(ToolVersion).from_statement(
        text(query)).params(tool_id=tool_id).all()
    DB.session.commit()
    tool_versions = [t.to_wire_as_client_tool_version() for t in tool_versions]
    resp = jsonify(tool_versions=tool_versions)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/task_template', methods=['GET'])
def get_task_template():
    logger.info(logging.myself())
    tool_version_id = request.args.get("tool_version_id")
    name = request.args.get("task_template_name")
    logger.debug(tool_version_id)

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
        name=name).one_or_none()
    if tt is not None:
        task_template_id = tt.id
    else:
        task_template_id = None

    resp = jsonify(task_template_id=task_template_id)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/task_template/<task_template_id>/version', methods=['GET'])
def get_task_template_version(task_template_id: int):
    logger.info(logging.myself())
    command_template = request.args.get("command_template")
    arg_mapping_hash = request.args.get("arg_mapping_hash")
    logger.debug(f"command_template={command_template}"
                 f"arg_mapping_hash={arg_mapping_hash}")

    # get task template version object
    query = """
    SELECT
        task_template_version.*
    FROM task_template_version
    WHERE
        task_template_id = :task_template_id
        AND command_template = :command_template
        AND arg_mapping_hash = :arg_mapping_hash
    """
    ttv = DB.session.query(TaskTemplateVersion).from_statement(text(query))\
        .params(
            task_template_id=task_template_id,
            command_template=command_template,
            arg_mapping_hash=arg_mapping_hash).one_or_none()

    if ttv is not None:
        wire_obj = ttv.to_wire_as_client_task_template_version()
    else:
        wire_obj = None

    resp = jsonify(task_template_version=wire_obj)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/node', methods=['GET'])
def get_node_id():
    """Get a node id: If a matching node isn't found, return None.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a node
        task_template_version_id: version id of the task_template a node
                                  belongs to.
    """
    logger.info(logging.myself())
    data = request.args
    logger.debug(data)

    query = """
        SELECT node.id
        FROM node
        WHERE
            node_args_hash = :node_args_hash
            AND task_template_version_id = :task_template_version_id"""
    result = DB.session.query(Node).from_statement(text(query)).params(
        node_args_hash=data['node_args_hash'],
        task_template_version_id=data['task_template_version_id']
    ).one_or_none()

    if result is None:
        resp = jsonify({'node_id': None})
    else:
        resp = jsonify({'node_id': result.id})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag', methods=['GET'])
def get_dag_id():
    """Get a dag id: If a matching dag isn't found, return None.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    logger.info(logging.myself())
    data = request.args
    logger.debug(data)

    query = """SELECT dag.id FROM dag WHERE hash = :dag_hash"""
    result = DB.session.query(Dag).from_statement(text(query)).params(
        dag_hash=data["dag_hash"]
    ).one_or_none()

    if result is None:
        resp = jsonify({'dag_id': None})
    else:
        resp = jsonify({'dag_id': result.id})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/task', methods=['GET'])
def get_task_id_and_status():
    logger.info(logging.myself())
    data = request.args
    logger.debug(data)

    query = """
        SELECT task.id, task.status
        FROM task
        WHERE
            workflow_id = :workflow_id
            AND node_id = :node_id
            AND task_args_hash = :task_args_hash
    """
    result = DB.session.query(Task).from_statement(text(query)).params(
        workflow_id=data['workflow_id'],
        node_id=data['node_id'],
        task_args_hash=data["task_args_hash"]
    ).one_or_none()

    # send back json
    if result is None:
        resp = jsonify({'task_id': None, 'task_status': None})
    else:
        resp = jsonify({'task_id': result.id, 'task_status': result.status})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow', methods=['GET'])
def get_workflow_id_and_status():
    logger.info(logging.myself())
    data = request.args
    logger.debug(data)

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
        tool_version_id=data['tool_version_id'],
        dag_id=data['dag_id'],
        workflow_args_hash=data['workflow_args_hash'],
        task_hash=data['task_hash']
    ).one_or_none()

    # send back json
    if result is None:
        resp = jsonify({'workflow_id': None, 'status': None})
    else:
        resp = jsonify({'workflow_id': result.id,
                        'status': result.status})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow/<workflow_args_hash>', methods=['GET'])
def get_matching_workflows_by_workflow_args(workflow_args_hash: int):
    """
    Return any dag hashes that are assigned to workflows with identical
    workflow args
    """
    logger.debug(logging.myself())

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
        workflow_args_hash=workflow_args_hash).all()
    DB.session.commit()
    res = [(row.task_hash, row.tool_version_id, row.hash) for row in res]
    resp = jsonify(matching_workflows=res)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/is_resumable', methods=['GET'])
def workflow_run_is_terminated(workflow_run_id: int):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))

    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
            AND (
                workflow_run.status = 'T'
                OR workflow_run.heartbeat_date <= UTC_TIMESTAMP()
            )
    """
    res = DB.session.query(WorkflowRun).from_statement(text(query)).params(
        workflow_run_id=workflow_run_id).one_or_none()
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


@jqs.route('/workflow_run_status', methods=['GET'])
def get_active_workflow_runs() -> Dict:
    """Return all workflow runs that are currently in the specified state."""
    logger.info(logging.myself())

    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.status in :workflow_run_status
    """
    workflow_runs = DB.session.query(WorkflowRun).from_statement(text(query))\
        .params(workflow_run_status=request.args.getlist('status')).all()
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp


# ############################ SCHEDULER ROUTES ###############################

@jqs.route('/workflow/<workflow_id>/queued_tasks/<n_queued_tasks>',
           methods=['GET'])
def get_queued_jobs(workflow_id: int, n_queued_tasks: int) -> Dict:
    """Returns oldest n tasks (or all tasks if total queued tasks < n) to be
    instantiated. Because the SGE can only qsub tasks at a certain rate, and we
    poll every 10 seconds, it does not make sense to return all tasks that are
    queued because only a subset of them can actually be instantiated
    Args:
        last_sync (datetime): time since when to get tasks
    """

    # TODO: this is where we would filter by task priority
    query = """
        SELECT
            task.*
        FROM
            task
        WHERE
            task.workflow_id = :workflow_id
            AND task.status = :task_status
        ORDER BY task.id
        LIMIT :n_queued_jobs"""
    tasks = DB.session.query(Task).from_statement(text(query)).params(
        workflow_id=workflow_id,
        task_status=TaskStatus.QUEUED_FOR_INSTANTIATION,
        n_queued_jobs=int(n_queued_tasks)
    ).all()
    DB.session.commit()
    task_dcts = [t.to_wire_as_executor_task() for t in tasks]
    resp = jsonify(task_dcts=task_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/get_suspicious_task_instances',
           methods=['GET'])
def get_suspicious_task_instances(workflow_run_id: int):
    # query all job instances that are submitted to executor or running which
    # haven't reported as alive in the allocated time.

    query = """
    SELECT
        task_instance.id, task_instance.workflow_run_id,
        task_instance.executor_id
    FROM
        task_instance
    WHERE
        task_instance.workflow_run_id = :workflow_run_id
        AND task_instance.status in :active_tasks
        AND task_instance.report_by_date <= UTC_TIMESTAMP()
    """
    rows = DB.session.query(TaskInstance).from_statement(text(query)).params(
        active_tasks=[TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                      TaskInstanceStatus.RUNNING],
        workflow_run_id=workflow_run_id
    ).all()
    DB.session.commit()
    resp = jsonify(task_instances=[ti.to_wire_as_executor_task_instance()
                                   for ti in rows])
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/get_task_instances_to_terminate',
           methods=['GET'])
def get_task_instances_to_terminate(workflow_run_id: int):
    workflow_run = DB.session.query(WorkflowRun).filter_by(
        id=workflow_run_id).one()

    if workflow_run.status == WorkflowRunStatus.HOT_RESUME:
        task_instance_states = [TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR]
    if workflow_run.status == WorkflowRunStatus.COLD_RESUME:
        task_instance_states = [TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                                TaskInstanceStatus.RUNNING]

    query = """
    SELECT
        task_instance.id, task_instance.workflow_run_id,
        task_instance.executor_id
    FROM
        task_instance
    WHERE
        task_instance.workflow_run_id = :workflow_run_id
        AND task_instance.status in :task_instance_states
    """
    rows = DB.session.query(TaskInstance).from_statement(text(query)).params(
        task_instance_states=task_instance_states,
        workflow_run_id=workflow_run_id
    ).all()
    DB.session.commit()
    resp = jsonify(task_instances=[ti.to_wire_as_executor_task_instance()
                                   for ti in rows])
    resp.status_code = StatusCodes.OK
    return resp


# ############################## SWARM ROUTES ################################

@jqs.route('/workflow/<workflow_id>/task_status_updates', methods=['POST'])
def get_task_by_status_only(workflow_id: int):
    """Returns all tasks in the database that have the specified status

    Args:
        status (str): status to query for
        last_sync (datetime): time since when to get tasks
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_id", workflow_id)
    data = request.get_json()

    last_sync = data['last_sync']
    swarm_tasks_tuples = data.get('swarm_tasks_tuples', [])
    logger.info("swarm_task_tuples: {}".format(swarm_tasks_tuples))

    # get time from db
    db_time = DB.session.execute("SELECT UTC_TIMESTAMP AS t").fetchone()['t']
    str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

    if swarm_tasks_tuples:
        swarm_tasks_tuples = [(int(task_id), str(status))
                              for task_id, status in swarm_tasks_tuples]
        # Sample swarm_tasks_tuples: [(1, 'I')]
        swarm_task_ids = ",".join(
            [str(task_id[0]) for task_id in swarm_tasks_tuples])
        swarm_tasks_tuples = [(int(task_id), str(status))
                              for task_id, status in swarm_tasks_tuples]

        query_swarm_tasks_tuples = ""
        for task_id, status in swarm_tasks_tuples:
            query_swarm_tasks_tuples += f"({task_id},'{status}'),"
        # get rid of trailing comma on final line
        query_swarm_tasks_tuples = query_swarm_tasks_tuples[:-1]
        logger.info(f"******* QUERY SERVER TASK BY STATUS, this is the query_swarm_tasks_tuple: {query_swarm_tasks_tuples} ********")

        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = {workflow_id}
                AND (
                    (
                        task.id IN ({swarm_task_ids})
                        AND (task.id, status) NOT IN ({tuples}))
                    OR status_date >= '{status_date}')
        """.format(workflow_id=workflow_id,
                   swarm_task_ids=swarm_task_ids,
                   tuples=query_swarm_tasks_tuples,
                   status_date=last_sync)
        logger.debug(query)
        rows = DB.session.query(Task).from_statement(text(query)).all()

    else:
        logger.info("********* NO SWARM TASKS SUPPLIED FOR UPDATE TASKS, PULLING ALL CHANGED WORKFLOWS *******")
        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = :workflow_id
                AND status_date >= :last_sync"""
        rows = DB.session.query(Task).from_statement(
            text(query)).params(workflow_id=workflow_id,
                                last_sync=str(last_sync)).all()

    DB.session.commit()
    task_dcts = [row.to_wire_as_swarm_task() for row in rows]
    logger.info(f"*** THIS IS THE QUERY SERVER (TASK_STATUS_UPDATE). THIS IS THE TASK_DCT: {task_dcts} ******")
    logger.info("task_dcts={}".format(task_dcts))
    resp = jsonify(task_dcts=task_dcts, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


# ############################## WORKER ROUTES ################################

@jqs.route('/task_instance/<task_instance_id>/kill_self', methods=['GET'])
def kill_self(task_instance_id: int):
    """Check a task instance's status to see if it needs to kill itself
    (state W, or L)"""
    kill_statuses = TaskInstance.kill_self_states
    logger.debug(logging.myself())
    logging.logParameter("task_instance_id", task_instance_id)

    # TODO: This select is a bit heavy weight for it's purpose
    query = """
        SELECT
            task_instance.id
        FROM
            task_instance
        WHERE
            task_instance.id = :task_instance_id
            AND task_instance.status in :statuses

    """
    should_kill = DB.session.query(TaskInstance).from_statement(
        text(query)).params(task_instance_id=task_instance_id,
                            statuses=kill_statuses).one_or_none()
    if should_kill is not None:
        resp = jsonify(should_kill=True)
    else:
        resp = jsonify()
    resp.status_code = StatusCodes.OK
    logger.debug(resp)
    return resp


@jqs.route('/task/<task_id>/most_recent_ti_error', methods=['GET'])
def get_most_recent_ji_error(task_id: int):
    """
    Route to determine the cause of the most recent task_instance's error
    :param task_id:
    :return: error message
    """

    logger.debug(logging.myself())
    logging.logParameter("task_id", task_id)

    query = """
        SELECT
            tiel.*
        FROM
            task_instance ti
        JOIN
            task_instance_error_log tiel
            ON ti.id = tiel.task_instance_id
        WHERE
            ti.task_id = :task_id
        ORDER BY
            ti.id desc, tiel.id desc
        LIMIT 1"""
    ti_error = DB.session.query(TaskInstanceErrorLog).from_statement(
        text(query)).params(task_id=task_id).one_or_none()
    DB.session.commit()
    if ti_error is not None:
        resp = jsonify({"error_description": ti_error.description})
    else:
        resp = jsonify({"error_description": ""})
    resp.status_code = StatusCodes.OK
    return resp
