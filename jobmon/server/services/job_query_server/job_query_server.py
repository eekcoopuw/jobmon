import logging
import os
from datetime import datetime
from sqlalchemy.orm import contains_eager

from flask import jsonify, request
from http import HTTPStatus

from jobmon.server.database import ScopedSession
from jobmon.models.job import Job
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.task_dag import TaskDagMeta
from jobmon.client.workflow.workflow import WorkflowDAO
from jobmon.client.workflow.workflow_run import WorkflowRunDAO, \
    WorkflowRunStatus
from jobmon.server.services.job_query_server import app

logger = logging.getLogger(__name__)


@app.teardown_appcontext
def shutdown_session(exception=None):
    ScopedSession.remove()


@app.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening"""
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_queued', methods=['GET'])
def get_queued_for_instantiation():
    jobs = ScopedSession.query(Job).filter_by(
        status=JobStatus.QUEUED_FOR_INSTANTIATION,
        dag_id=request.args['dag_id']).all()
    job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_submitted_or_running', methods=['GET'])
def get_submitted_or_running():
    instances = ScopedSession.query(JobInstance).\
        filter(
            JobInstance.status.in_([
                JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                JobInstanceStatus.RUNNING])).\
        join(Job).\
        options(contains_eager(JobInstance.job)).\
        filter_by(dag_id=request.args['dag_id']).all()
    instances = [i.to_wire() for i in instances]
    resp = jsonify(ji_dcts=instances)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_jobs', methods=['GET'])
def get_jobs():
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id:
    """
    jobs = ScopedSession.query(Job).filter(
        Job.dag_id == request.args['dag_id']).all()
    job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_timed_out', methods=['GET'])
def get_timed_out():
    running = ScopedSession.query(JobInstance).\
        filter(
            JobInstance.status.in_([
                JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                JobInstanceStatus.RUNNING])).\
        join(Job).\
        options(contains_eager(JobInstance.job)).\
        filter(Job.dag_id == request.args['dag_id'],
               Job.max_runtime != None).all()  # noqa: E711
    now = datetime.utcnow()
    timed_out = [r.to_wire() for r in running
                 if (now - r.status_date).seconds > r.job.max_runtime]
    resp = jsonify(timed_out=timed_out)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_dag_ids_by_hash', methods=['GET'])
def get_dag_ids_by_hash():
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id:
    """
    dags = ScopedSession.query(TaskDagMeta).filter(
        TaskDagMeta.dag_hash == request.args['dag_hash']).all()
    dag_ids = [dag.dag_id for dag in dags]
    resp = jsonify(dag_ids=dag_ids)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_workflows_by_inputs', methods=['GET'])
def get_workflows_by_inputs():
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id:
    """
    workflow = ScopedSession.query(WorkflowDAO).\
        filter(WorkflowDAO.dag_id == request.args['dag_id']).\
        filter(WorkflowDAO.workflow_args == request.args['workflow_args'])\
        .first()
    if workflow:
        resp = jsonify(workflow_dct=workflow.to_wire())
        resp.status_code = HTTPStatus.OK
        return resp
    else:
        return '', HTTPStatus.NO_CONTENT


@app.route('/is_workflow_running', methods=['GET'])
def is_workflow_running():
    """Check if a previous workflow run for your user is still running """
    wf_run = (ScopedSession.query(WorkflowRunDAO).filter_by(
        workflow_id=request.args['workflow_id'],
        status=WorkflowRunStatus.RUNNING,
    ).order_by(WorkflowRunDAO.id.desc()).first())
    if not wf_run:
        return jsonify(is_running=False, workflow_run_dct={})
    resp = jsonify(is_running=True, workflow_run_dct=wf_run.to_wire())
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_job_instances_of_workflow_run', methods=['GET'])
def get_job_instances_of_workflow_run():
    jis = ScopedSession.query(JobInstance).filter_by(
        workflow_run_id=request.args['workflow_run_id']).all()
    jis = [ji.to_wire() for ji in jis]
    resp = jsonify(job_instances=jis)
    resp.status_code = HTTPStatus.OK
    return resp
