import logging
import os
from datetime import datetime
from sqlalchemy.orm import contains_eager

from flask import jsonify, Flask, request
from http import HTTPStatus

from jobmon.database import ScopedSession
from jobmon.models import Job, JobInstance, JobStatus, JobInstanceStatus
from jobmon.meta_models import TaskDagMeta
from jobmon.workflow.workflow import WorkflowDAO
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


logger = logging.getLogger(__name__)

app = Flask(__name__)


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


@app.route('/dag/<dag_id>/job', methods=['GET'])
def get_jobs_by_status(dag_id):
    if request.args.get('status', None) is not None:
        jobs = ScopedSession.query(Job).filter_by(
            status=request.args['status'],
            dag_id=dag_id).all()
    else:
        jobs = ScopedSession.query(Job).filter_by(dag_id=dag_id).all()
    job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/dag/<dag_id>/job_instance', methods=['GET'])
def get_job_instances_by_filter(dag_id):
    if request.args.get('runtime', None) is not None:
        instances = ScopedSession.query(JobInstance).\
            filter(
                JobInstance.status.in_(request.args.getlist('status'))).\
            join(Job).\
            options(contains_eager(JobInstance.job)).\
            filter(Job.dag_id == dag_id,
                   Job.max_runtime != None).all()  # noqa: E711
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
        instances = [i.to_wire() for i in instances]
    resp = jsonify(ji_dcts=instances)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/dag', methods=['GET'])
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
    dag_ids = [dag.dag_id for dag in dags]
    resp = jsonify(dag_ids=dag_ids)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/dag/<dag_id>/workflow', methods=['GET'])
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
    if workflow:
        resp = jsonify(workflow_dct=workflow.to_wire())
        resp.status_code = HTTPStatus.OK
        return resp
    else:
        return '', HTTPStatus.NO_CONTENT


@app.route('/workflow/<workflow_id>/workflow_run', methods=['GET'])
def is_workflow_running(workflow_id):
    """Check if a previous workflow run for your user is still running """
    wf_run = (ScopedSession.query(WorkflowRunDAO).filter_by(
        workflow_id=workflow_id,
        status=WorkflowRunStatus.RUNNING,
    ).order_by(WorkflowRunDAO.id.desc()).first())
    if not wf_run:
        return jsonify(is_running=False, workflow_run_dct={})
    resp = jsonify(is_running=True, workflow_run_dct=wf_run.to_wire())
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/workflow_run/<workflow_run_id>/job_instance', methods=['GET'])
def get_job_instances_of_workflow_run(workflow_run_id):
    jis = ScopedSession.query(JobInstance).filter_by(
        workflow_run_id=workflow_run_id).all()
    jis = [ji.to_wire() for ji in jis]
    resp = jsonify(job_instances=jis)
    resp.status_code = HTTPStatus.OK
    return resp
