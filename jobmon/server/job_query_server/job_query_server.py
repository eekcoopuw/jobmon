from http import HTTPStatus as StatusCodes
import os
from typing import Dict

from flask import jsonify, request, Blueprint
from sqlalchemy.orm import contains_eager
from sqlalchemy.sql import func, text

from jobmon.models import DB
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.node import Node
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.task_template import TaskTemplate
from jobmon.models.task_template_version import TaskTemplateVersion
from jobmon.models.tool import Tool
from jobmon.models.tool_version import ToolVersion
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.server_logging import jobmonLogging as logging

jqs = Blueprint("job_query_server", __name__)

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
    time = DB.session.execute("select UTC_TIMESTAMP as time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


def get_time(session):
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


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
def get_tool_versions(tool_id):
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


@jqs.route('/task_template/<task_template_name>', methods=['GET'])
def get_task_template(task_template_name: str):
    logger.info(logging.myself())
    tool_version_id = request.args.get("tool_version_id")
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
        name=task_template_name).one_or_none()
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
    logger.debug(command_template, arg_mapping_hash)

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


@jqs.route('/workflow/<workflow_id>/workflow_attribute', methods=['GET'])
def get_workflow_attribute(workflow_id):
    """Get a particular attribute of a particular workflow

    Args:
        workflow_id: id of the workflow to retrieve workflow_attributes for
        workflow_attribute_type: num_age_groups, num_locations, etc.
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_id", workflow_id)
    workflow_attribute_type = request.args.get('workflow_attribute_type', None)
    if workflow_attribute_type:
        attribute = (DB.session.query(WorkflowAttribute).join(Workflow)
                     .filter(Workflow.id == workflow_id,
                             WorkflowAttribute.attribute_type ==
                             workflow_attribute_type)
                     ).all()
    else:
        attribute = (DB.session.query(WorkflowAttribute).join(Workflow)
                     .filter(Workflow.id == workflow_id)
                     ).all()
    DB.session.commit()
    attr_dcts = [w.to_wire() for w in attribute]
    logger.info("workflow_attr_dct={}".format(attr_dcts))
    resp = jsonify(workflow_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow/<workflow_id>/job_attribute', methods=['GET'])
def get_job_attribute_by_workflow(workflow_id):
    """Get a particular attribute of a particular type of job in the workflow

    Args:
        workflow_id: id of the workflow to retrieve workflow_attributes for
        job_type: type of job getting attributes for
        job_attribute_type: num_locations, wallclock, etc.
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_id", workflow_id)
    job_attribute_type = request.args.get('job_attribute_type', None)
    if job_attribute_type:
        attribute = (DB.session.query(JobAttribute).join(Job)
                     .join(TaskDagMeta)
                     .join(Workflow)
                     .filter(Workflow.id == workflow_id,
                             JobAttribute.attribute_type == job_attribute_type)
                     ).all()
    else:
        attribute = (DB.session.query(JobAttribute).join(Job)
                     .join(TaskDagMeta)
                     .join(Workflow)
                     .filter(Workflow.id == workflow_id)).all()
    DB.session.commit()
    attr_dcts = [j.to_wire() for j in attribute]
    logger.info("job_attr_dct={}".format(attr_dcts))
    resp = jsonify(job_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/job_attribute', methods=['GET'])
def get_job_attribute(job_id):
    """Get a particular attribute of a particular type of job in the workflow

    Args:
        job_id: id of the job to retrieve job for
        job_attribute_type: num_locations, wallclock, etc.
    """
    logger.debug(logging.myself())
    logging.logParameter("job_id", job_id)
    job_attribute_type = request.args.get('job_attribute_type', None)
    if job_attribute_type:
        attribute = (DB.session.query(JobAttribute).join(Job)
                     .filter(Job.job_id == job_id,
                             JobAttribute.attribute_type == job_attribute_type)
                     ).all()
    else:
        attribute = (DB.session.query(JobAttribute).join(Job)
                     .filter(Job.job_id == job_id)
                     ).all()
    DB.session.commit()
    attr_dcts = [j.to_wire() for j in attribute]
    logger.info("job_attr_dct={}".format(attr_dcts))
    resp = jsonify(job_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


# @jqs.route('/dag/<dag_id>/job', methods=['GET'])
# def get_jobs_by_status(dag_id):
#     """Returns all jobs in the database that have the specified status

#     Args:
#         status (str): status to query for
#         last_sync (datetime): time since when to get jobs
#     """
#     logger.debug(logging.myself())
#     logging.logParameter("dag_id", dag_id)
#     last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
#     time = get_time(DB.session)
#     if request.args.get('status', None) is not None:
#         jobs = DB.session.query(Job).filter(
#             Job.dag_id == dag_id,
#             Job.status == request.args['status'],
#             Job.status_date >= last_sync).all()
#     else:
#         jobs = DB.session.query(Job).filter(
#             Job.dag_id == dag_id,
#             Job.status_date >= last_sync).all()
#     DB.session.commit()
#     job_dcts = [j.to_wire() for j in jobs]
#     logger.info("job_attr_dct={}".format(job_dcts))
#     resp = jsonify(job_dcts=job_dcts, time=time)
#     resp.status_code = StatusCodes.OK
#     return resp


@jqs.route('/dag/<dag_id>/queued_jobs/<n_queued_jobs>', methods=['GET'])
def get_queued_jobs(dag_id: int, n_queued_jobs: int) -> Dict:
    """Returns oldest n jobs (or all jobs if total queued jobs < n) to be
    instantiated. Because the SGE can only qsub jobs at a certain rate, and we
    poll every 10 seconds, it does not make sense to return all jobs that are
    queued because only a subset of them can actually be instantiated
    Args:
        last_sync (datetime): time since when to get jobs
    """
    last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
    time = get_time(DB.session)
    jobs = DB.session.query(Job). \
        filter(
            Job.dag_id == dag_id,
            Job.status == JobStatus.QUEUED_FOR_INSTANTIATION,
            Job.status_date >= last_sync).\
        order_by(Job.job_id).\
        limit(n_queued_jobs)
    DB.session.commit()
    job_dcts = [j.to_wire_as_executor_job() for j in jobs]
    resp = jsonify(job_dcts=job_dcts, time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/job_status', methods=['GET'])
def get_jobs_by_status_only(dag_id):
    """Returns all jobs in the database that have the specified status

    Args:
        status (str): status to query for
        last_sync (datetime): time since when to get jobs
    """
    logger.debug(logging.myself())
    logging.logParameter("dag_id", dag_id)
    last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
    time = get_time(DB.session)
    if request.args.get('status', None) is not None:
        # select docker.job.job_id, docker.job.job_hash, docker.job.status from
        # docker.job where  docker.job.dag_id=1 and docker.job.status="G";
        # vs.
        # select docker.job.job_id, docker.job.job_hash, docker.job.status from
        # docker.job where  docker.job.status="G" and docker.job.dag_id=1;
        # 0.000 sec vs 0.015 sec (result from MySQL WorkBench)
        # Thus move the dag_id in front of status in the filter
        rows = DB.session.query(Job).with_entities(Job.job_id, Job.status,
                                                   Job.job_hash).filter(
            Job.dag_id == dag_id,
            Job.status == request.args['status'],
            Job.status_date >= last_sync).all()
    else:
        rows = DB.session.query(Job).with_entities(Job.job_id, Job.status,
                                                   Job.job_hash).filter(
            Job.dag_id == dag_id,
            Job.status_date >= last_sync).all()
    DB.session.commit()
    job_dcts = [Job(job_id=row[0], status=row[1], job_hash=row[2]
                    ).to_wire_as_swarm_job() for row in rows]
    logger.info("job_attr_dct={}".format(job_dcts))
    resp = jsonify(job_dcts=job_dcts, time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/get_timed_out_executor_ids', methods=['GET'])
def get_timed_out_executor_ids(dag_id):
    """This function isnt used by SGE because it automatically terminates timed
     out jobs, however if an executor is being used that does not automatically
    terminate timed out jobs, do it here. Finds all jobs that have been in the
    submitted or running state for longer than the maximum specified run
    time"""
    jiid_exid_tuples = DB.session.query(JobInstance). \
        filter_by(dag_id=dag_id). \
        filter(JobInstance.status.in_(
        [JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RUNNING])). \
        join(ExecutorParameterSet). \
        options(contains_eager(JobInstance.executor_parameter_set)). \
        filter(ExecutorParameterSet.max_runtime_seconds != None). \
        filter(
        func.timediff(func.UTC_TIMESTAMP(), JobInstance.status_date) >
        func.SEC_TO_TIME(ExecutorParameterSet.max_runtime_seconds)). \
        with_entities(JobInstance.job_instance_id, JobInstance.executor_id). \
        all()  # noqa: E711
    DB.session.commit()

    # TODO: convert to executor_job_instance wire format
    resp = jsonify(jiid_exid_tuples=jiid_exid_tuples)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/get_job_instances_by_status', methods=['GET'])
def get_job_instances_by_status(dag_id):
    """Returns all job_instances in the database that have the specified filter

    Args:
        dag_id (int): dag_id to which the job_instances are attached
        status (list): list of statuses to query for

    Return:
        list of tuples (job_instance_id, executor_id) whose runtime is above
        max_runtime_seconds
    """
    logger.debug(logging.myself())
    logging.logParameter("dag_id", dag_id)
    job_instances = DB.session.query(JobInstance). \
        filter_by(dag_id=dag_id). \
        filter(JobInstance.status.in_(request.args.getlist('status'))). \
        all()  # noqa: E711
    DB.session.commit()
    resp = jsonify(job_instances=[ji.to_wire() for ji in job_instances])
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/get_suspicious_job_instances', methods=['GET'])
def get_suspicious_job_instances(dag_id):
    # query all job instances that are submitted to executor or running which
    # haven't reported as alive in the allocated time.
    # ignore job instances created after heartbeat began. We'll reconcile them
    # during the next reconciliation loop.
    rows = DB.session.query(JobInstance). \
        join(TaskDagMeta). \
        filter_by(dag_id=dag_id). \
        filter(JobInstance.status.in_([
        JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
        JobInstanceStatus.RUNNING])). \
        filter(JobInstance.submitted_date <= TaskDagMeta.heartbeat_date). \
        filter(JobInstance.report_by_date <= func.UTC_TIMESTAMP()). \
        with_entities(JobInstance.job_instance_id, JobInstance.executor_id). \
        all()
    DB.session.commit()
    job_instances = [JobInstance(job_instance_id=row[0], executor_id=row[1])
                     for row in rows]
    resp = jsonify(job_instances=[ji.to_wire_as_executor_job_instance()
                                  for ji in job_instances])
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag', methods=['GET'])
def get_dags_by_inputs():
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id: id of the dag to retrieve
    """
    logger.debug(logging.myself())
    if request.args.get('dag_hash', None) is not None:
        dags = DB.session.query(TaskDagMeta).filter(
            TaskDagMeta.dag_hash == request.args['dag_hash']).all()
    else:
        dags = DB.session.query(TaskDagMeta).all()
    DB.session.commit()
    dag_ids = [dag.dag_id for dag in dags]
    resp = jsonify(dag_ids=dag_ids)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow/workflow_args', methods=['GET'])
def get_workflow_args():
    """
    Return any dag hashes that are assigned to workflows with identical
    workflow args
    """
    logger.debug(logging.myself())
    workflow_args = request.args['workflow_args']
    workflow_hashes = DB.session.query(Workflow).filter(
        Workflow.workflow_args == workflow_args).\
        with_entities(Workflow.workflow_hash).all()
    DB.session.commit()
    resp = jsonify(workflow_hashes=workflow_hashes)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/workflow', methods=['GET'])
def get_workflows_by_inputs(dag_id):
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id: id of the dag to retrieve
    """
    logger.debug(logging.myself())
    logging.logParameter("dag_id", dag_id)
    workflow = DB.session.query(Workflow). \
        filter(Workflow.dag_id == dag_id). \
        filter(Workflow.workflow_args == request.args['workflow_args']
               ).first()
    DB.session.commit()
    if workflow:
        resp = jsonify(workflow_dct=workflow.to_wire())
        resp.status_code = StatusCodes.OK
        return resp
    else:
        return '', StatusCodes.NO_CONTENT


@jqs.route('/workflow/<workflow_id>/workflow_run', methods=['GET'])
def is_workflow_running(workflow_id):
    """Check if a previous workflow run for your user is still running

    Args:
        workflow_id: id of the workflow to check if its previous workflow_runs
        are running
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_id", workflow_id)
    wf_run = (DB.session.query(WorkflowRunDAO).filter_by(
        workflow_id=workflow_id,
        status=WorkflowRunStatus.RUNNING,
    ).order_by(WorkflowRunDAO.id.desc()).first())
    DB.session.commit()
    if not wf_run:
        return jsonify(is_running=False, workflow_run_dct={})
    resp = jsonify(is_running=True, workflow_run_dct=wf_run.to_wire())
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/job_instance', methods=['GET'])
def get_job_instances_of_workflow_run(workflow_run_id):
    """Get all job_instances of a particular workflow run

    Args:
        workflow_run_id: id of the workflow_run to retrieve job_instances for
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_run_id", workflow_run_id)
    jis = DB.session.query(JobInstance).filter_by(
        workflow_run_id=workflow_run_id).all()
    jis = [ji.to_wire_as_executor_job_instance() for ji in jis]
    DB.session.commit()
    resp = jsonify(job_instances=jis)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job_instance/<job_instance_id>/kill_self', methods=['GET'])
def kill_self(job_instance_id):
    """Check a job instance's status to see if it needs to kill itself
    (state W, or L)"""
    kill_statuses = JobInstance.kill_self_states
    logger.debug(logging.myself())
    logging.logParameter("job_instance_id", job_instance_id)
    should_kill = DB.session.query(JobInstance). \
        filter_by(job_instance_id=job_instance_id). \
        filter(JobInstance.status.in_(kill_statuses)).first()
    if should_kill:
        resp = jsonify(should_kill=True)
    else:
        resp = jsonify()
    resp.status_code = StatusCodes.OK
    logger.debug(resp)
    return resp


@jqs.route('/job/<executor_id>/get_resources', methods=['GET'])
def get_resources(executor_id):
    """
    This route is created for testing purpose

    :param executor_id:
    :return:
    """
    logger.debug(logging.myself())
    query = f"SELECT m_mem_free, num_cores, max_runtime_seconds FROM " \
        f"job_instance, job WHERE job_instance.job_id=job.job_id " \
        f"AND executor_id = {executor_id}"
    res = DB.session.execute(query).fetchone()
    DB.session.commit()
    resp = jsonify({'mem': res[0], 'cores': res[1], 'runtime': res[2]})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/most_recent_ji_error', methods=['GET'])
def get_most_recent_ji_error(job_id: int):
    """
    Route to determine the cause of the most recent job_instance's error
    :param job_id:
    :return: error message
    """

    logger.debug(logging.myself())
    logging.logParameter("job_id", job_id)

    query = """
        SELECT
            jiel.*
        FROM
            job_instance ji
        JOIN
            job_instance_error_log jiel
            ON ji.job_instance_id = jiel.job_instance_id
        WHERE
            ji.job_id = :job_id
        ORDER BY
            ji.job_instance_id desc, jiel.id desc
        LIMIT 1"""
    ji_error = DB.session.query(JobInstanceErrorLog).from_statement(
        text(query)).params(job_id=job_id).one_or_none()
    DB.session.commit()
    if ji_error is not None:
        resp = jsonify({"error_description": ji_error.description})
    else:
        resp = jsonify({"error_description": ""})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job_instance/<job_instance_id>/get_executor_id', methods=['GET'])
def get_executor_id(job_instance_id: int):
    """
    This route is to get the executor id by job_instance_id

    :param job_instance_id:
    :return: executor_id
    """
    logger.debug(logging.myself())
    sql = "SELECT executor_id FROM job_instance WHERE job_instance_id={}".format(job_instance_id)
    try:
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        resp = jsonify({"executor_id": res[0]})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        resp = jsonify({'msg': str(e)})
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp


@jqs.route('/node', methods=['GET'])
def get_node():
    """Get a node:

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a node
        task_template_version_id: version id of the task_template a node
                                  belongs to.
    """
    logger.info(logging.myself())
    data = request.args
    logger.debug(data)

    query = """
        SELECT id
        FROM node
        WHERE node_args_hash = :node_args_hash 
            AND task_template_version_id = :task_template_version_id
        LIMIT 1"""

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


@jqs.route('/job_instance/<job_instance_id>/get_nodename', methods=['GET'])
def get_nodename(job_instance_id: int):
    """
    This route is to get the nodename by job_instance_id

    :param job_instance_id:
    :return: nodename
    """
    logger.debug(logging.myself())
    sql = "SELECT nodename FROM job_instance WHERE job_instance_id={}".format(job_instance_id)
    try:
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        resp = jsonify({"nodename": res[0]})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        resp = jsonify({'msg': str(e)})
        resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        return resp


@jqs.route('/job_instance/<job_instance_id>/get_errors', methods=['GET'])
def get_ji_error(job_instance_id: int):
    """
    This route is created for testing purpose

    :param executor_id:
    :return:
    """
    logger.debug(logging.myself())
    query = f"SELECT description FROM job_instance_error_log WHERE job_instance_id = {job_instance_id};"
    result = DB.session.execute(query)
    errors = []
    for r in result:
        errors.append(r[0])
    DB.session.commit()
    logger.debug(errors)
    resp = jsonify({'errors': errors})
    resp.status_code = StatusCodes.OK
    return resp
