from http import HTTPStatus as StatusCodes
import os
from typing import Dict

from flask import jsonify, request, Blueprint
from sqlalchemy.orm import contains_eager
from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.jobmonLogging import jobmonLogging as logging


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
    jobs = DB.session.query(Job).\
        filter(
            Job.dag_id == dag_id,
            Job.status.in_([JobStatus.QUEUED_FOR_INSTANTIATION,
                            JobStatus.ADJUSTING_RESOURCES]),
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
        filter_by(dag_id=dag_id).\
        filter(JobInstance.status.in_(
            [JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
             JobInstanceStatus.RUNNING])).\
        join(ExecutorParameterSet).\
        options(contains_eager(JobInstance.executor_parameter_set)).\
        filter(ExecutorParameterSet.max_runtime_seconds != None).\
        filter(
            func.timediff(func.UTC_TIMESTAMP(), JobInstance.status_date) >
            func.SEC_TO_TIME(ExecutorParameterSet.max_runtime_seconds)).\
        with_entities(JobInstance.job_instance_id, JobInstance.executor_id).\
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
    job_instances = DB.session.query(JobInstance).\
        filter_by(dag_id=dag_id).\
        filter(JobInstance.status.in_(request.args.getlist('status'))).\
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
    rows = DB.session.query(JobInstance).\
        join(TaskDagMeta).\
        filter_by(dag_id=dag_id).\
        filter(JobInstance.status.in_([
            JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
            JobInstanceStatus.RUNNING])).\
        filter(JobInstance.submitted_date <= TaskDagMeta.heartbeat_date).\
        filter(JobInstance.report_by_date <= func.UTC_TIMESTAMP()).\
        with_entities(JobInstance.job_instance_id, JobInstance.executor_id).\
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
    workflow = DB.session.query(Workflow).\
        filter(Workflow.dag_id == dag_id).\
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
    should_kill = DB.session.query(JobInstance).\
        filter_by(job_instance_id=job_instance_id).\
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
    query = f"select m_mem_free, num_cores, max_runtime_seconds from " \
            f"job_instance, job where job_instance.job_id=job.job_id " \
            f"and executor_id = {executor_id}"
    res = DB.session.execute(query).fetchone()
    DB.session.commit()
    resp = jsonify({'mem': res[0], 'cores': res[1], 'runtime': res[2]})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/most_recent_exec_id', methods=['GET'])
def get_most_recent_exec_id(job_id: int):
    """
    Route to collect the most recent executor id for a given job so that it
    can be qacct'd on to determine more detailed exit information after the fact
    :param job_id:
    :return: executor_id
    """
    logger.debug(logging.myself())
    executor_id = DB.session.query(JobInstance). \
                  filter_by(job_id=job_id). \
                  order_by(JobInstance.status.desc()).\
                  with_entities(JobInstance.executor_id).first()
    DB.session.commit()
    resp = jsonify(executor_id=executor_id)
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
    sql = "select executor_id from job_instance where job_instance_id={}".format(job_instance_id)
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


@jqs.route('/job_instance/<job_instance_id>/get_nodename', methods=['GET'])
def get_nodename(job_instance_id: int):
    """
    This route is to get the nodename by job_instance_id

    :param job_instance_id:
    :return: nodename
    """
    logger.debug(logging.myself())
    sql = "select nodename from job_instance where job_instance_id={}".format(job_instance_id)
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
