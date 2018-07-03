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


logger = logging.getLogger(__name__)

app = Flask(__name__)


def flask_thread():
    app.run(host="0.0.0.0", port=config.jqs_port, debug=True,
            use_reloader=False, threaded=True)


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


@app.route('/get_queued_for_instantiation', methods=['GET'])
def get_queued_for_instantiation():
    with session_scope() as session:
        jobs = session.query(Job).filter_by(
            status=JobStatus.QUEUED_FOR_INSTANTIATION,
            dag_id=request.form['dag_id']).all()
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
            filter_by(dag_id=request.form['dag_id']).all()
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
            Job.dag_id == request.form['dag_id']).all()
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
            filter(Job.dag_id == request.form['dag_id'],
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
            TaskDagMeta.dag_hash == request.form['dag_hash']).all()
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
            filter(WorkflowDAO.dag_id == request.form['dag_id']).\
            filter(WorkflowDAO.workflow_args == request.form['workflow_args'])\
            .first()
        if workflow:
            resp = jsonify(workflow_dct=workflow.to_wire())
            resp.status_code = HTTPStatus.OK
        else:
            resp = no_results()
    return resp
