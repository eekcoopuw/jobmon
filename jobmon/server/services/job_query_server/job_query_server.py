import logging
import os
from datetime import datetime
from sqlalchemy.orm import contains_eager

from flask import jsonify, request, Blueprint
from http import HTTPStatus

from jobmon.server.database import ScopedSession
from jobmon.models.job import Job
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.task_dag import TaskDagMeta
from jobmon.client.swarm.workflow.workflow import WorkflowDAO
from jobmon.client.swarm.workflow.workflow_run import WorkflowRunDAO, \
    WorkflowRunStatus

jqs = Blueprint("job_query_server", __name__)

logger = logging.getLogger(__name__)


@jqs.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening"""
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = HTTPStatus.OK
    return resp


@jqs.route('/dag/<dag_id>/job', methods=['GET'])
def get_jobs_by_status(dag_id):
    if request.args.get('status', None) is not None:
        jobs = ScopedSession.query(Job).filter_by(
            status=request.args['status'],
            dag_id=dag_id).all()
    else:
        jobs = ScopedSession.query(Job).filter_by(dag_id=dag_id).all()
    ScopedSession.commit()
    job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@jqs.route('/dag/<dag_id>/job_instance', methods=['GET'])
def get_job_instances_by_filter(dag_id):
    if request.args.get('runtime', None) is not None:
        instances = ScopedSession.query(JobInstance).\
            filter(
                JobInstance.status.in_(request.args.getlist('status'))).\
            join(Job).\
            options(contains_eager(JobInstance.job)).\
            filter(Job.dag_id == dag_id,
                   Job.max_runtime != None).all()  # noqa: E711
        ScopedSession.commit()
        now = datetime.utcnow()
        instances = [r.to_wire() for r in instances
                     if (now - r.status_date).seconds > r.job.max_runtime]
    else:
        instances = ScopedSession.query(JobInstance).\
            filter(
                JobInstance.status.in_(request.args.getlist('status'))).\
            join(Job).\
            options(contains_eager(JobInstance.job)).\
            filter_by(dag_id=dag_id).all()
        ScopedSession.commit()
        instances = [i.to_wire() for i in instances]
    resp = jsonify(ji_dcts=instances)
    resp.status_code = HTTPStatus.OK
    return resp


@jqs.route('/dag', methods=['GET'])
def get_dags_by_inputs():
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id:
    """
    if request.args.get('dag_hash', None) is not None:
        dags = ScopedSession.query(TaskDagMeta).filter(
            TaskDagMeta.dag_hash == request.args['dag_hash']).all()
    else:
        dags = ScopedSession.query(TaskDagMeta).all()
    ScopedSession.commit()
    dag_ids = [dag.dag_id for dag in dags]
    resp = jsonify(dag_ids=dag_ids)
    resp.status_code = HTTPStatus.OK
    return resp


@jqs.route('/dag/<dag_id>/workflow', methods=['GET'])
def get_workflows_by_inputs(dag_id):
    """
    Return a dictionary mapping job_id to a dict of the job's instance
    variables

    Args
        dag_id:
    """
    workflow = ScopedSession.query(WorkflowDAO).\
        filter(WorkflowDAO.dag_id == dag_id).\
        filter(WorkflowDAO.workflow_args == request.args['workflow_args']
               ).first()
    ScopedSession.commit()
    if workflow:
        resp = jsonify(workflow_dct=workflow.to_wire())
        resp.status_code = HTTPStatus.OK
        return resp
    else:
        return '', HTTPStatus.NO_CONTENT


@jqs.route('/workflow/<workflow_id>/workflow_run', methods=['GET'])
def is_workflow_running(workflow_id):
    """Check if a previous workflow run for your user is still running """
    wf_run = (ScopedSession.query(WorkflowRunDAO).filter_by(
        workflow_id=workflow_id,
        status=WorkflowRunStatus.RUNNING,
    ).order_by(WorkflowRunDAO.id.desc()).first())
    ScopedSession.commit()
    if not wf_run:
        return jsonify(is_running=False, workflow_run_dct={})
    resp = jsonify(is_running=True, workflow_run_dct=wf_run.to_wire())
    resp.status_code = HTTPStatus.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/job_instance', methods=['GET'])
def get_job_instances_of_workflow_run(workflow_run_id):
    jis = ScopedSession.query(JobInstance).filter_by(
        workflow_run_id=workflow_run_id).all()
    jis = [ji.to_wire() for ji in jis]
    ScopedSession.commit()
    resp = jsonify(job_instances=jis)
    resp.status_code = HTTPStatus.OK
    return resp
