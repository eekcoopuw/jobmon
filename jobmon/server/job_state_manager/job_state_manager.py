from datetime import datetime
from flask import jsonify, request, Blueprint
from http import HTTPStatus as StatusCodes
import json
import os
import socket
from sqlalchemy.sql import func, text
import traceback
from typing import Optional
import warnings
import sqlalchemy
from sqlalchemy.sql import text

from jobmon import config
from jobmon.models import DB
from jobmon.models.arg import Arg
from jobmon.models.arg_type import ArgType
from jobmon.models.command_template_arg_type_mapping import \
    CommandTemplateArgTypeMapping
from jobmon.models.attributes.constants import job_attribute, qsub_attribute
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.attributes.workflow_run_attribute import \
    WorkflowRunAttribute
from jobmon.models.dag import Dag
from jobmon.models.edge import Edge
from jobmon.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.node import Node
from jobmon.models.node_arg import NodeArg
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.task_template import TaskTemplate
from jobmon.models.task_template_version import TaskTemplateVersion
from jobmon.models.tool import Tool
from jobmon.models.tool_version import ToolVersion
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.server.server_side_exception import log_and_raise


jsm = Blueprint("job_state_manager", __name__)


# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)


def mogrify(topic, msg):
    """json encode the message and prepend the topic.
    see: https://stackoverflow.com/questions/25188792/
    how-can-i-use-send-json-with-pyzmq-pub-sub
    """

    return str(topic) + ' ' + json.dumps(msg)


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


@jsm.route('/tool', methods=['POST'])
def add_tool():
    """Add a tool to the database"""
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    tool = Tool(name=data["name"])
    DB.session.add(tool)
    DB.session.commit()
    tool = tool.to_wire_as_client_tool()
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


def _add_or_get_arg(name):
    try:
        query = """
        SELECT id, name
        FROM arg
        WHERE name = :name
        """
        arg = DB.session.query(TaskTemplate).from_statement(text(query))\
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


@jsm.route('/job', methods=['POST'])
def add_job():
    """Add a job to the database

    Args:
        name: name for the job
        job_hash: unique hash for the job
        command: job's command
        dag_id: dag_id to which this job is attached
        max_attempts: how many times the job should be attempted
        tag: job attribute tag
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    job = Job(
        dag_id=data['dag_id'],
        name=data['name'],
        tag=data.get('tag', None),
        job_hash=data['job_hash'],
        command=data['command'],
        max_attempts=data.get('max_attempts', 3),
        status=JobStatus.REGISTERED)
    DB.session.add(job)
    DB.session.commit()

    job_dct = job.to_wire_as_swarm_job()
    resp = jsonify(job_dct=job_dct)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/node', methods=['POST'])
def add_client_node_and_node_args():
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


@jsm.route('/client_dag/<dag_hash>', methods=['POST'])
def add_client_dag(dag_hash):
    """Add a new dag to the database.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    logger.info(logging.myself())

    # add dag
    dag = Dag(hash=dag_hash)
    DB.session.add(dag)
    DB.session.commit()

    # return result
    resp = jsonify(dag_id=dag.id)
    resp.status_code = StatusCodes.OK

    return resp


@jsm.route('/edge/<dag_id>', methods=['POST'])
def add_edges(dag_id):
    """Add a group of edges to the database.

    Args:
        dag_id: identifies the dag whose edges are being inserted
        nodes_and_edges: a json object with the following format:
            {
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

    #data = json.loads(data['nodes_and_edges'])

    for node_id, edges in data.items():
        logger.debug(f'!!Edges: {edges}')
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


@jsm.route('/task_dag', methods=['POST'])
def add_task_dag():
    """Add a task_dag to the database

    Args:
        name: name for the task_dag
        user: name of the user of the dag
        dag_hash: unique hash for the task_dag
    """
    logger.info(logging.myself())
    data = request.get_json(force=True)
    logger.debug(data)
    dag = TaskDagMeta(
        name=data['name'],
        user=data['user'],
        dag_hash=data['dag_hash'])
    DB.session.add(dag)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()
    dag_id = dag.dag_id
    resp = jsonify(dag_id=dag_id)
    resp.status_code = StatusCodes.OK
    return resp


def _get_workflow_run_id(job):
    """Return the workflow_run_id by job_id"""
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_id", job.job_id))
    wf = DB.session.query(Workflow).filter_by(dag_id=job.dag_id).first()
    if not wf:
        DB.session.commit()
        return None  # no workflow has started, so no workflow run
    wf_run = (DB.session.query(WorkflowRunDAO).
              filter_by(workflow_id=wf.id).
              order_by(WorkflowRunDAO.id.desc()).first())
    wf_run_id = wf_run.id
    DB.session.commit()
    return wf_run_id


@jsm.route('/job_instance', methods=['POST'])
def add_job_instance():
    """Add a job_instance to the database

    Args:
        job_id (int): unique id for the job
        executor_type (str): string name of the executor type used
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    logger.debug("Add JI for job {}".format(data['job_id']))

    # query job
    job = DB.session.query(Job).filter_by(job_id=data['job_id']).first()
    DB.session.commit()

    # create job_instance from job parameters
    job_instance = JobInstance(
        executor_type=data['executor_type'],
        job_id=data['job_id'],
        dag_id=job.dag_id,
        workflow_run_id=_get_workflow_run_id(job),
        executor_parameter_set_id=job.executor_parameter_set_id)
    DB.session.add(job_instance)
    DB.session.commit()

    try:
        job_instance.job.transition(JobStatus.INSTANTIATED)
    except InvalidStateTransition:
        if job_instance.job.status == JobStatus.INSTANTIATED:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   "{}'s job_instance_id {} from I to I"
                   .format(data['job_id'], job_instance.job_instance_id))
            logger.warning(msg)
        else:
            raise
    finally:
        DB.session.commit()
    resp = jsonify(
        job_instance=job_instance.to_wire_as_executor_job_instance())
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow', methods=['POST', 'PUT'])
def add_update_workflow():
    """Add a workflow to the database or update it (via PUT)

    Args:
        dag_id (int): dag_id to which this workflow is attached
        workflow_args: unique args for the workflow
        workflow_hash: unique hash for the workflow
        name (str): name for the workflow
        user (str): name of the user of the workflow
        description (str): string description of the workflow, optional
        any other Workflow attributes you want to set
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    if request.method == 'POST':
        wf = Workflow(dag_id=data['dag_id'],
                      workflow_args=data['workflow_args'],
                      workflow_hash=data['workflow_hash'],
                      name=data['name'],
                      user=data['user'],
                      description=data.get('description', ""))
        DB.session.add(wf)
        logger.debug(logging.logParameter("DB.session", DB.session))
    else:
        wf_id = data.pop('wf_id')
        wf = DB.session.query(Workflow).\
            filter(Workflow.id == wf_id).first()
        for key, val in data.items():
            setattr(wf, key, val)
    DB.session.commit()
    wf_dct = wf.to_wire()
    resp = jsonify(workflow_dct=wf_dct)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/error_logger', methods=['POST'])
def workflow_error_logger():
    logger.info(logging.myself())
    data = request.get_json()
    logger.error(data["traceback"])
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run', methods=['POST', 'PUT'])
def add_update_workflow_run():
    """Add a workflow to the database or update it (via PUT)

    Args:
        workflow_id (int): workflow_id to which this workflow_run is attached
        user (str): name of the user of the workflow
        hostname (str): host on which this workflow_run was run
        pid (str): process_id where this workflow_run is/was run
        stderr (str): where stderr should be directed
        stdout (str): where stdout should be directed
        project (str): sge project where this workflow_run should be run
        slack_channel (str): channel where this workflow_run should send
            notifications
        resource adjustment (float): rate at which the resources will be
            increased if the jobs fail from under-requested resources
        any other Workflow attributes you want to set
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(data)
    if request.method == 'POST':
        wfr = WorkflowRunDAO(workflow_id=data['workflow_id'],
                             user=data['user'],
                             hostname=data['hostname'],
                             pid=data['pid'],
                             stderr=data['stderr'],
                             stdout=data['stdout'],
                             working_dir=data['working_dir'],
                             project=data['project'],
                             slack_channel=data['slack_channel'],
                             executor_class=data['executor_class'])
        workflow = DB.session.query(Workflow).\
            filter(Workflow.id == data['workflow_id']).first()
        # Set all previous runs to STOPPED
        for run in workflow.workflow_runs:
            run.status = WorkflowRunStatus.STOPPED
        DB.session.add(wfr)
        logger.debug(logging.logParameter("DB.session", DB.session))
    else:
        wfr = DB.session.query(WorkflowRunDAO).\
            filter(WorkflowRunDAO.id == data['workflow_run_id']).first()
        for key, val in data.items():
            setattr(wfr, key, val)
    DB.session.commit()
    wfr_id = wfr.id
    resp = jsonify(workflow_run_id=wfr_id)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_done', methods=['POST'])
def log_done(job_instance_id):
    """Log a job_instance as done
    Args:

        job_instance_id: id of the job_instance to log done
    """
    logger.info(logging.myself())
    data = request.get_json()
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    logger.debug("Log DONE for JI {}".format(job_instance_id))
    logger.debug("Data: " + str(data))
    ji = _get_job_instance(DB.session, job_instance_id)
    if data.get('executor_id', None) is not None:
        ji.executor_id = data['executor_id']
    if data.get('nodename', None) is not None:
        ji.nodename = data['nodename']
    logger.debug("log_done nodename: {}".format(ji.nodename))
    logger.debug(logging.logParameter("DB.session", DB.session))
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.DONE)
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


def _log_error(ji: JobInstance,
               error_state: int,
               error_msg: str,
               executor_id: Optional[int] = None,
               nodename: Optional[str] = None
               ):
    if nodename is not None:
        ji.nodename = nodename
    if executor_id is not None:
        ji.executor_id = executor_id

    try:
        error = JobInstanceErrorLog(job_instance_id=ji.job_instance_id,
                                       description=error_msg)
        DB.session.add(error)
        msg = _update_job_instance_state(ji, error_state)
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except InvalidStateTransition as e:
        DB.session.rollback()
        logger.warning(str(e))
        log_msg = f"JSM::log_error(), reason={msg}"
        warnings.warn(log_msg)
        logger.debug(log_msg)
        raise
    except Exception as e:
        DB.session.rollback()
        logger.warning(str(e))
        raise


    return resp


@jsm.route('/job_instance/<job_instance_id>/log_error_worker_node',
           methods=['POST'])
def log_error_worker_node(job_instance_id: int):
    """Log a job_instance as errored
    Args:

        job_instance_id (str): id of the job_instance to log done
        error_message (str): message to log as error
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    error_state = data["error_state"]
    error_message = data["error_message"]
    logger.debug(error_message)
    logger.debug(str(len(error_message)))
    executor_id = data.get('executor_id', None)
    nodename = data.get("nodename", None)
    logger.debug(f"Log ERROR for JI:{job_instance_id} message={error_message}")
    logger.debug("data:" + str(data))

    ji = _get_job_instance(DB.session, job_instance_id)
    try:
        resp = _log_error(ji, error_state, error_message, executor_id, nodename)
        return resp
    except sqlalchemy.exc.OperationalError as e:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ji, error_state, new_msg, executor_id, nodename)
        return resp


@jsm.route('/job_instance/<job_instance_id>/log_error_reconciler',
           methods=['POST'])
def log_error_reconciler(job_instance_id: int):
    """Log a job_instance as errored
    Args:
        job_instance:
        data:
        oom_killed: whether or not given job errored due to an oom-kill event
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    error_state = data['error_state']
    error_message = data['error_message']
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    logger.debug(f"Log ERROR for JI:{job_instance_id} message={error_message}")
    logger.debug("data:" + str(data))

    ji = _get_job_instance(DB.session, job_instance_id)

    # make sure the job hasn't logged a new heartbeat since we began
    # reconciliation
    if ji.report_by_date <= datetime.utcnow():
        try:
            resp = _log_error(ji, error_state, error_message, executor_id,
                              nodename)
        except sqlalchemy.exc.OperationalError as e:
            # modify the error message and retry
            new_msg = error_message.encode("latin1", "replace").decode("utf-8")
            resp = _log_error(ji, error_state, new_msg, executor_id, nodename)
    else:
        resp = jsonify()
        resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_error_health_monitor',
           methods=['POST'])
def log_error_health_monitor(job_instance_id: int):
    """Log a job_instance as errored
    Args:
        job_instance:
        data:
        oom_killed: whether or not given job errored due to an oom-kill event
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    error_state = data['error_state']
    error_message = data.get('error_message', 'Health monitor synced the state from QPID')
    executor_id = data.get('executor_id', None)
    nodename = data.get('nodename', None)
    logger.debug(f"Log ERROR for JI:{job_instance_id} message={error_message}")
    logger.debug("data:" + str(data))

    ji = _get_job_instance(DB.session, job_instance_id)

    resp = _log_error(ji, error_state, error_message, executor_id, nodename)
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_no_exec_id', methods=['POST'])
def log_no_exec_id(job_instance_id):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    logger.debug(f"Log NO EXECUTOR ID for JI {job_instance_id}")
    data = request.get_json()
    if data['executor_id'] == qsub_attribute.NO_EXEC_ID:
        logger.info("Qsub was unsuccessful and caused an exception")
    else:
        logger.info("Qsub may have run, but the job id could not be parsed "
                    "from the qsub response so no executor id can be assigned"
                    " at this time")
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    msg = _update_job_instance_state(ji, JobInstanceStatus.NO_EXECUTOR_ID)
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/log_oom/<executor_id>', methods=['POST'])
def log_oom(executor_id: str):
    """Log instances where a job_instance is killed by an Out of Memory Kill
    event TODO: factor log_error out as a function and use it for both log_oom and log_error
    Args:
        executor_id (int): A UGE job_id
        task_id (int): UGE task_id if the job was an array job (included as
                       JSON in request)
        error_message (str): Optional message to log (included as JSON in
                             request)
    """
    logger.info(logging.myself())
    data = request.get_json()

    # TODO: figure out what to do with log_oom
    logger.error(f"{executor_id} ran out of memory. {data}")
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
    # job_instance = _get_job_instance_by_executor_id(DB.session,
    #                                                 int(executor_id))

    # return _log_error(job_instance=job_instance, data=data, oom_killed=True)


@jsm.route('/job_instance/<job_instance_id>/log_executor_id', methods=['POST'])
def log_executor_id(job_instance_id):
    """Log a job_instance's executor id
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    report_by_date = func.ADDTIME(
        func.UTC_TIMESTAMP(),
        func.SEC_TO_TIME(data["next_report_increment"]))
    logger.debug("Log EXECUTOR_ID for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    logger.info("in log_executor_id, ji is {}".format(repr(ji)))
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    _update_job_instance(ji, executor_id=data['executor_id'],
                         report_by_date=report_by_date)
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/log_running', methods=['POST'])
def log_dag_running(dag_id: int):
    """Log a dag as running

    Args:
        dag_id: id of the dag to move to running
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))

    params = {"dag_id": int(dag_id)}
    query = """
        UPDATE workflow
        SET status = 'R'
        WHERE dag_id = :dag_id"""
    DB.session.execute(query, params)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/log_heartbeat', methods=['POST'])
def log_dag_heartbeat(dag_id):
    """Log a dag as being responsive, with a heartbeat
    Args:

        dag id: id of the job_instance to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))

    params = {"dag_id": int(dag_id)}
    query = """
        UPDATE task_dag
        SET heartbeat_date = UTC_TIMESTAMP()
        WHERE dag_id in (:dag_id)"""
    DB.session.execute(query, params)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/log_executor_report_by', methods=['POST'])
def log_executor_report_by(dag_id):
    logger.info(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    data = request.get_json()

    params = {}
    for key in ["next_report_increment", "executor_ids"]:
        params[key] = data[key]

    if params["executor_ids"]:
        query = """
            UPDATE job_instance
            SET report_by_date = ADDTIME(
                UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE executor_id in :executor_ids"""
        DB.session.execute(query, params)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_report_by', methods=['POST'])
def log_ji_report_by(job_instance_id):
    """Log a job_instance as being responsive with a new report_by_date, this
    is done at the worker node heartbeat_interval rate, so it may not happen at
    the same rate that the reconciler updates batch submitted report_by_dates
    (also because it causes a lot of traffic if all workers are logging report
    _by_dates often compared to if the reconciler runs often)
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    executor_id = data.get('executor_id', None)
    params = {}
    params["next_report_increment"] = data["next_report_increment"]
    params["job_instance_id"] = job_instance_id
    if executor_id is not None:
        params["executor_id"] = executor_id
        query = """
                UPDATE job_instance
                SET report_by_date = ADDTIME(
                    UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment)),
                    executor_id = :executor_id
                WHERE job_instance_id = :job_instance_id"""
    else:
        query = """
            UPDATE job_instance
            SET report_by_date = ADDTIME(
                UTC_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
            WHERE job_instance_id = :job_instance_id"""
    DB.session.execute(query, params)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_running', methods=['POST'])
def log_running(job_instance_id):
    """Log a job_instance as running
    Args:

        job_instance_id: id of the job_instance to log as running
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log RUNNING for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    msg = _update_job_instance_state(ji, JobInstanceStatus.RUNNING)
    if data.get('nodename', None) is not None:
        ji.nodename = data['nodename']
    logger.debug("log_running nodename: {}".format(ji.nodename))
    ji.process_group_id = data['process_group_id']
    ji.report_by_date = func.ADDTIME(
        func.UTC_TIMESTAMP(), func.SEC_TO_TIME(data['next_report_increment']))
    if data.get('executor_id', None) is not None:
        ji.executor_id = data['executor_id']
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_nodename', methods=['POST'])
def log_nodename(job_instance_id):
    """Log a job_instance's nodename'
    Args:

        job_instance_id: id of the job_instance to log done
        nodename (str): name of the node on which the job_instance is running
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log nodename for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    logger.debug(" ;;;;;;;;;;; log_nodename nodename: {}".format(data[
        'nodename']))
    _update_job_instance(ji, nodename=data['nodename'])
    DB.session.commit()
    resp = jsonify(message='')
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_usage', methods=['POST'])
def log_usage(job_instance_id):
    """Log the usage stats of a job_instance
    Args:

        job_instance_id: id of the job_instance to log done
        usage_str (str, optional): stats such as maxrss, etc
        wallclock (str, optional): wallclock of running job
        maxvmem (str, optional): max virtual memory used
        cpu (str, optional): cpu used
        io (str, optional): io used
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    if data.get('maxrss', None) is None:
        data['maxrss'] = '-1'

    keys_to_attrs = {data.get('usage_str', None): job_attribute.USAGE_STR,
                     data.get('wallclock', None): job_attribute.WALLCLOCK,
                     data.get('cpu', None): job_attribute.CPU,
                     data.get('io', None): job_attribute.IO,
                     data.get('maxrss', None): job_attribute.MAXRSS}

    logger.debug("usage_str is {}, wallclock is {}, maxrss is {}, cpu is {}, "
                 "io is {}".format(data.get('usage_str', None),
                                   data.get('wallclock', None),
                                   data.get('maxrss', None),
                                   data.get('cpu', None),
                                   data.get('io', None)))
    job_instance = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    job_id = job_instance.job_id
    msg = _update_job_instance(job_instance,
                               usage_str=data.get('usage_str', None),
                               wallclock=data.get('wallclock', None),
                               maxrss=data.get('maxrss', None),
                               cpu=data.get('cpu', None),
                               io=data.get('io', None))
    for k in keys_to_attrs:
        logger.debug(
            'The value of {kval} being set in the attribute table is {k}'.
            format(kval=keys_to_attrs[k], k=k))
        if k is not None:
            ja = (JobAttribute(
                job_id=job_id, attribute_type=keys_to_attrs[k], value=k))
            DB.session.add(ja)
        else:
            logger.debug('The value has not been set, nothing to upload')
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job/<job_id>/queue', methods=['POST'])
def queue_job(job_id):
    """Queue a job and change its status
    Args:

        job_id: id of the job to queue
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))
    job = DB.session.query(Job)\
        .filter_by(job_id=job_id).first()
    try:
        job.transition(JobStatus.QUEUED_FOR_INSTANTIATION)
    except InvalidStateTransition:
        if job.status == JobStatus.QUEUED_FOR_INSTANTIATION:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   "{} from Q to Q".format(job_id))
            logger.warning(msg)
        else:
            raise
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job/<job_id>/update_job', methods=['POST'])
def update_job(job_id):
    """
    Change the non-dag forming job parameters before resuming
    Args:
        job_id (int): id of the job for which parameters are updated
        tag (str): a group identifier
        max_attempts (int): maximum numver of attempts before sending the job
            to the ERROR FATAL state
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))

    data = request.get_json()
    tag = data.get('tag', None)
    max_attempts = data.get('max_attempts', 3)

    update_job = """
                 UPDATE job
                 SET tag=:tag, max_attempts=:max_attempts
                 WHERE job_id=:job_id
                 """
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.execute(update_job,
                       {"tag": tag,
                        "max_attempts": max_attempts,
                        "job_id": job_id})
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job/<job_id>/update_resources', methods=['POST'])
def update_job_resources(job_id):
    """ Change the resources set for a given job

    Args:
        job_id (int): id of the job for which resources will be changed
        parameter_set_type (str): parameter set type for this job
        max_runtime_seconds (int, optional): amount of time job is allowed to
            run for
        context_args (dict, optional): unstructured parameters to pass to
            executor
        queue (str, optional): sge queue to submit jobs to
        num_cores (int, optional): how many cores to get from sge
        m_mem_free ():
        j_resource (bool, optional): whether to request access to the j drive
        resource_scales (dict): values to scale by upon resource error
        hard_limit (bool): whether to move queues if requester resources exceed
            queue limits
    """

    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))

    data = request.get_json()
    parameter_set_type = data["parameter_set_type"]

    exec_params = ExecutorParameterSet(
        job_id=job_id,
        parameter_set_type=parameter_set_type,
        max_runtime_seconds=data.get('max_runtime_seconds', None),
        context_args=data.get('context_args', None),
        queue=data.get('queue', None),
        num_cores=data.get('num_cores', None),
        m_mem_free=data.get('m_mem_free', 2),
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


@jsm.route('/job/<job_id>/reset', methods=['POST'])
def reset_job(job_id):
    """Reset a job and change its status
    Args:

        job_id: id of the job to reset
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))
    job = DB.session.query(Job).filter_by(job_id=job_id).first()
    logger.debug(logging.logParameter("DB.session", DB.session))
    job.reset()
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/reset_incomplete_jobs', methods=['POST'])
def reset_incomplete_jobs(dag_id):
    """Reset all jobs of a dag and change their statuses
    Args:

        dag_id: id of the dag to reset
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    time = get_time(DB.session)
    up_job = """
        UPDATE job
        SET status=:registered_status, num_attempts=0, status_date='{}'
        WHERE dag_id=:dag_id
        AND job.status!=:done_status
    """.format(time)
    log_errors = """
            INSERT INTO job_instance_error_log
                (job_instance_id, description, error_time)
            SELECT job_instance_id,
            CONCAT('Job RESET requested setting to E from status of: ',
                   job_instance.status) as description,
            UTC_TIMESTAMP as error_time
            FROM job_instance
            JOIN job USING(job_id)
            WHERE job.dag_id=:dag_id
            AND job.status!=:done_status
        """
    up_job_instance = """
        UPDATE job_instance
        JOIN job USING(job_id)
        SET job_instance.status=:error_status,
            job_instance.status_date=UTC_TIMESTAMP
        WHERE job.dag_id=:dag_id
        AND job.status!=:done_status
    """
    logger.debug("Query:\n{}".format(up_job))
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.execute(
        up_job,
        {"dag_id": dag_id,
         "registered_status": JobStatus.REGISTERED,
         "done_status": JobStatus.DONE})
    logger.debug("Query:\n{}".format(log_errors))
    DB.session.execute(
        log_errors,
        {"dag_id": dag_id,
         "done_status": JobStatus.DONE})
    logger.debug("Query:\n{}".format(up_job_instance))
    DB.session.execute(
        up_job_instance,
        {"dag_id": dag_id,
         "error_status": JobInstanceStatus.ERROR,
         "done_status": JobStatus.DONE})
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


def _get_job_instance(session, job_instance_id):
    """Return a JobInstance from the database

    Args:
        session: DB.session or Session object to use to connect to the db
        job_instance_id (int): job_instance_id with which to query the database
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("session", session))
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    job_instance = session.query(JobInstance).filter_by(
        job_instance_id=job_instance_id).first()
    return job_instance


def _get_job_instance_by_executor_id(session, executor_id):
    """Return a JobInstance from the database

    Args:
        session: DB.session or Session object to use to connect to the db
        executor_id (int): executor_id with which to query the database
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("session", session))
    logger.debug(logging.logParameter("executor_id", executor_id))
    job_instance = session.query(JobInstance).filter_by(
        executor_id=executor_id).first()
    return job_instance


def _update_job_instance_state(job_instance, status_id):
    """Advance the states of job_instance and it's associated Job,
    return any messages that should be published based on
    the transition

    Args:
        job_instance (obj) object of time models.JobInstance
        status_id (int): id of the status to which to transition
    """
    logger.info(logging.myself())
    logger.debug(f"Update JI state {status_id} for {job_instance}")
    response = ""
    try:
        job_instance.transition(status_id)
    except InvalidStateTransition:
        if job_instance.status == status_id:
            # It was already in that state, just log it
            msg = f"Attempting to transition to existing state." \
                f"Not transitioning job, jid= " \
                f"{job_instance.job_instance_id}" \
                f"from {job_instance.status} to {status_id}"
            logger.warning(msg)
        else:
            # Tried to move to an illegal state
            msg = f"Illegal state transition. " \
                f"Not transitioning job, jid= " \
                f"{job_instance.job_instance_id}, " \
                f"from {job_instance.status} to {status_id}"
            # log_and_raise(msg, logger)
            logger.error(msg)
    except KillSelfTransition:
        msg = f"kill self, cannot transition " \
              f"jid={job_instance.job_instance_id}"
        logger.warning(msg)
        response = "kill self"
    except Exception as e:
        msg = f"General exception in _update_job_instance_state, " \
            f"jid {job_instance}, transitioning to {job_instance}. " \
            f"Not transitioning job. {e}"
        log_and_raise(msg, logger)

    job = job_instance.job

    # ... see tests/tests_job_state_manager.py for Event example
    if job.status in [JobStatus.DONE, JobStatus.ERROR_FATAL]:
        to_publish = mogrify(job.dag_id, (job.job_id, job.status))
        return to_publish
    else:
        return response


def _update_job_instance(job_instance, **kwargs):
    """Set attributes on a job_instance, primarily status

    Args:
        job_instance (obj): object of type models.JobInstance
    """
    logger.info(logging.myself())
    logger.debug(logging.logParameter("job_instance", job_instance))
    logger.debug("Update JI  {}".format(job_instance))
    status_requested = kwargs.get('status', None)
    logger.debug(logging.logParameter("status_requested", status_requested))
    if status_requested is not None:
        logger.debug("status_requested:{s}; job_instance.status:{j}".format
                     (s=status_requested, j=job_instance.status))
        if status_requested == job_instance.status:
            kwargs.pop(status_requested)
            logger.debug("Caught InvalidStateTransition. Not transitioning "
                         "job_instance {} from {} to {}."
                         .format(job_instance.job_instance_id,
                                 job_instance.status, status_requested))
    for k, v in kwargs.items():
        setattr(job_instance, k, v)
    return


@jsm.route('/workflow_attribute', methods=['POST'])
def add_workflow_attribute():
    """Set attributes on a workflow

    Args:
        workflow_id (int): id of the workflow on which to set attributes
        attribute_type (obj): object of type WorkflowAttribute
        value (str): value of the WorkflowAttribute to add
    """
    logger.info(logging.myself())
    data = request.get_json()
    workflow_attribute = WorkflowAttribute(
        workflow_id=data['workflow_id'],
        attribute_type=data['attribute_type'],
        value=data['value'])
    logger.debug(workflow_attribute)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.add(workflow_attribute)
    DB.session.commit()
    resp = jsonify({'workflow_attribute_id': workflow_attribute.id})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run_attribute', methods=['POST'])
def add_workflow_run_attribute():
    """Set attributes on a workflow_run

    Args:
        workflow_run_id (int): id of the workflow_run on which to set
        attributes
        attribute_type (obj): object of type WorkflowRunAttribute
        value (str): value of the WorkflowRunAttribute to add
    """
    logger.info(logging.myself())
    data = request.get_json()
    workflow_run_attribute = WorkflowRunAttribute(
        workflow_run_id=data['workflow_run_id'],
        attribute_type=data['attribute_type'],
        value=data['value'])
    logger.debug(workflow_run_attribute)
    DB.session.add(workflow_run_attribute)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()
    resp = jsonify({'workflow_run_attribute_id': workflow_run_attribute.id})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_attribute', methods=['POST'])
def add_job_attribute():
    """Set attributes on a job

    Args:
        job_id (int): id of the job on which to set attributes
        attribute_type (obj): object of type JobAttribute
        value (str): value of the JobAttribute to add
    """
    logger.info(logging.myself())
    data = request.get_json()
    job_attribute = JobAttribute(
        job_id=data['job_id'],
        attribute_type=data['attribute_type'],
        value=data['value'])
    logger.debug(job_attribute)
    DB.session.add(job_attribute)
    DB.session.commit()
    resp = jsonify({'job_attribute_id': job_attribute.id})
    resp.status_code = StatusCodes.OK
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
def set_log_level(level):
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
def attach_remote_syslog(level, host, port, sockettype):
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
def set_log_level_flask(level):
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
