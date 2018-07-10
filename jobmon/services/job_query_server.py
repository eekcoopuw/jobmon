import logging
import os
from datetime import datetime
from sqlalchemy.orm import contains_eager

from flask import jsonify, Flask, request
from http import HTTPStatus

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.models import Job, JobInstance, JobStatus, JobInstanceStatus
from jobmon.meta_models import TaskDagMeta
from jobmon.workflow.workflow import WorkflowDAO
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


logger = logging.getLogger(__name__)

app = Flask(__name__)


def flask_thread():
    app.run(host="0.0.0.0", port=config.jqs_port, debug=True)


@app.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening"""
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = HTTPStatus.OK
    return resp


@app.errorhandler(404)
def no_results(error=None):
    message = {'message': 'Results not found {}'.format(error)}
    resp = jsonify(message)
    resp.status_code = 404

    return resp


@app.route('/get_queued', methods=['GET'])
def get_queued_for_instantiation():
    with session_scope() as session:
        jobs = session.query(Job).filter_by(
            status=JobStatus.QUEUED_FOR_INSTANTIATION,
            dag_id=request.args['dag_id']).all()
        job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_submitted_or_running', methods=['GET'])
def get_submitted_or_running():
    with session_scope() as session:
        instances = session.query(JobInstance).\
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
    with session_scope() as session:
        jobs = session.query(Job).filter(
            Job.dag_id == request.args['dag_id']).all()
        job_dcts = [j.to_wire() for j in jobs]
    resp = jsonify(job_dcts=job_dcts)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/get_timed_out', methods=['GET'])
def get_timed_out():
    with session_scope() as session:
        running = session.query(JobInstance).\
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
    with session_scope() as session:
        dags = session.query(TaskDagMeta).filter(
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
    with session_scope() as session:
        workflow = session.query(WorkflowDAO).\
            filter(WorkflowDAO.dag_id == request.args['dag_id']).\
            filter(WorkflowDAO.workflow_args == request.args['workflow_args'])\
            .first()
        if workflow:
            resp = jsonify(workflow_dct=workflow.to_wire())
            resp.status_code = HTTPStatus.OK
        else:
            resp = no_results()
    return resp


@app.route('/workflow_running', methods=['GET'])
def is_workflow_running():
    """Check if a previous workflow run for your user is still running """
    print(request.args)
    with session_scope() as session:
        wf_run = (session.query(WorkflowRunDAO).filter_by(
            workflow_id=request.args['workflow_id'],
            status=WorkflowRunStatus.RUNNING,
        ).order_by(WorkflowRunDAO.id.desc()).first())
        if not wf_run:
            return jsonify(status=False, workflow_run_id=None,
                           hostname=None, pid=None, user=None)
        wf_run_id = wf_run.id
        hostname = wf_run.hostname
        pid = wf_run.pid
        user = wf_run.user
    resp = jsonify(status=True, workflow_run_id=wf_run_id,
                   hostname=hostname, pid=pid, user=user)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/sge_ids_of_previous_workflow_run', methods=['GET'])
def get_sge_ids_of_previous_workflow_run():
    with session_scope() as session:
        jis = session.query(JobInstance).filter_by(
            workflow_run_id=request.args['workflow_run_id']).all()
        sge_ids = [ji.executor_id for ji in jis]
    resp = jsonify(sge_ids=sge_ids)
    resp.status_code = HTTPStatus.OK
    return resp


if __name__ == '__main__':
    flask_thread()
