import logging
import os
from datetime import datetime

from flask import jsonify, Flask, request
from http import HTTPStatus

from jobmon import models
from jobmon.database import session_scope
from jobmon.pubsub_helpers import mogrify
from jobmon.config import config
from jobmon.meta_models import task_dag
from jobmon.workflow.workflow import WorkflowDAO
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus

# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)

app = Flask(__name__)


def start():
    app.run(host="0.0.0.0", port=config.jsm_port, debug=True, threaded=True)


@app.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening"""
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/add_job', methods=['POST'])
def add_job():
    job = models.Job(
        name=request.form['name'],
        job_hash=request.form['job_hash'],
        command=request.form['command'],
        dag_id=request.form['dag_id'],
        slots=request.form.get('slots', 1),
        mem_free=request.form.get('mem_free', 2),
        max_attempts=request.form.get('max_attempts', 1),
        max_runtime=request.form.get('max_runtime', None),
        context_args=request.form.get('context_args', "{}"),
        tag=request.form.get('tag', None),
        status=models.JobStatus.REGISTERED)
    with session_scope() as session:
        session.add(job)
        session.commit()
        job_id = job.job_id
    resp = jsonify(job_id=job_id)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/add_task_dag', methods=['POST'])
def add_task_dag():
    dag = task_dag.TaskDagMeta(
        name=request.form['name'],
        user=request.form['user'],
        dag_hash=request.form['dag_hash'],
        created_date=request.form['created_date'])
    with session_scope() as session:
        session.add(dag)
        session.commit()
        dag_id = dag.dag_id
    resp = jsonify(dag_id=dag_id)
    resp.status_code = HTTPStatus.OK
    return resp


def _get_workflow_run_id(job_id):
    with session_scope() as session:
        job = session.query(models.Job).filter_by(
            job_id=job_id).first()
        wf = session.query(WorkflowDAO).filter_by(dag_id=job.dag_id
                                                  ).first()
        if not wf:
            return None  # no workflow has started, so no workflow run
        wf_run = (session.query(WorkflowRunDAO).
                  filter_by(workflow_id=wf.id).
                  order_by(WorkflowRunDAO.id.desc()).first())
        wf_run_id = wf_run.id
    return wf_run_id


@app.route('/add_job_instance', methods=['POST'])
def add_job_instance():
    logger.debug("Add JI for job {}".format(request.form['job_id']))
    workflow_run_id = _get_workflow_run_id(request.form['job_id'])
    job_instance = models.JobInstance(
        executor_type=request.form['executor_type'],
        job_id=request.form['job_id'],
        workflow_run_id=workflow_run_id)
    with session_scope() as session:
        session.add(job_instance)
        session.commit()
        ji_id = job_instance.job_instance_id

        # TODO: Would prefer putting this in the model, but can't find the
        # right post-create hook. Investigate.
        job_instance.job.transition(models.JobStatus.INSTANTIATED)
    resp = jsonify(job_instance_id=ji_id)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/add_workflow', methods=['POST'])
def add_workflow():
    wf = WorkflowDAO(dag_id=request.form['dag_id'],
                     workflow_args=request.form['workflow_args'],
                     workflow_hash=request.form['workflow_hash'],
                     name=request.form['name'],
                     user=request.form['user'],
                     description=request.form.get('description', ""))
    with session_scope() as session:
        session.add(wf)
        session.commit()
        wf_dct = wf.to_wire()
    resp = jsonify(workflow_dct=wf_dct)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/add_workflow_run', methods=['POST'])
def add_workflow_run():
    wfr = WorkflowRunDAO(workflow_id=request.form['workflow_id'],
                         user=request.form['user'],
                         hostname=request.form['hostname'],
                         pid=request.form['pid'],
                         stderr=request.form['stderr'],
                         stdout=request.form['stdout'],
                         project=request.form['project'],
                         slack_channel=request.form['slack_channel'])
    with session_scope() as session:
        workflow = session.query(WorkflowDAO).\
            filter(WorkflowDAO.id == request.form['workflow_id']).first()
        # Set all previous runs to STOPPED
        for run in workflow.workflow_runs:
            run.status = WorkflowRunStatus.STOPPED
        session.add(wfr)
        session.commit()
        wfr_id = wfr.id
    resp = jsonify(workflow_run_id=wfr_id)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/update_workflow', methods=['POST'])
def update_workflow():
    with session_scope() as session:
        wf = session.query(WorkflowDAO).\
            filter(WorkflowDAO.id == request.form['wf_id']).first()
        wf.status = request.form['status']
        wf.status_date = datetime.utcnow()
        session.commit()
        wf_dct = wf.to_wire()
    resp = jsonify(workflow_dct=wf_dct)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/update_workflow_run', methods=['POST'])
def update_workflow_run():
    with session_scope() as session:
        wfr = session.query(WorkflowRunDAO).\
            filter(WorkflowRunDAO.id == request.form['wfr_id']).first()
        wfr.status = request.form['status']
        wfr.status_date = datetime.utcnow()
        session.commit()
    resp = jsonify(status=request.form['status'])
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/workflow_running', methods=['GET'])
def is_workflow_running():
    """Check if a previous workflow run for your user is still running """
    with session_scope() as session:
        wf_run = (session.query(WorkflowRunDAO).filter_by(
            workflow_id=request.form['workflow_id'],
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
        jis = session.query(models.JobInstance).filter_by(
            workflow_run_id=request.form['workflow_run_id']).all()
        sge_ids = [ji.executor_id for ji in jis]
    resp = jsonify(sge_ids=sge_ids)
    resp.status_code = HTTPStatus.OK
    return resp


@app.route('/log_done', methods=['POST'])
def log_done():
    logger.debug("Log DONE for JI {}".format(request.form['job_instance_id']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        msg = _update_job_instance_state(
            session, ji, models.JobInstanceStatus.DONE)
    return msg, 200


@app.route('/log_error', methods=['POST'])
def log_error():
    logger.debug("Log ERROR for JI {}, message={}".format(
        request.form['job_instance_id'], request.form['error_message']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        msg = _update_job_instance_state(
            session, ji, models.JobInstanceStatus.ERROR)
        error = models.JobInstanceErrorLog(
            job_instance_id=request.form['job_instance_id'],
            description=request.form['error_message'])
        session.add(error)
    return msg, 200


@app.route('/log_executor_id', methods=['POST'])
def log_executor_id():
    logger.debug("Log EXECUTOR_ID for JI {}"
                 .format(request.form['job_instance_id']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        msg = _update_job_instance_state(
            session, ji,
            models.JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
        _update_job_instance(session, ji,
                             executor_id=request.form['executor_id'])
    return msg, 200


@app.route('/log_heartbeat', methods=['POST'])
def log_heartbeat(self,):
    with session_scope() as session:
        dag = session.query(task_dag.TaskDagMeta).filter_by(
            dag_id=request.form['dag_id']).first()
        if dag:
            dag.heartbeat_date = datetime.utcnow()
            session.commit()
    return "", 200


@app.route('/log_running', methods=['POST'])
def log_running():
    logger.debug("Log RUNNING for JI {}"
                 .format(request.form['job_instance_id']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        msg = _update_job_instance_state(
            session, ji, models.JobInstanceStatus.RUNNING)
        ji.nodename = request.form['nodename']
        ji.process_group_id = request.form['process_group_id']
    return msg, 200


@app.route('/log_nodename', methods=['POST'])
def log_nodename():
    logger.debug("Log USAGE for JI {}".format(request.form['job_instance_id']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        _update_job_instance(session, ji, nodename=request.form['nodename'])
    return "", 200


@app.route('/log_usage', methods=['POST'])
def log_usage():
    logger.debug("Log USAGE for JI {}".format(request.form['job_instance_id']))
    with session_scope() as session:
        ji = _get_job_instance(session, request.form['job_instance_id'])
        _update_job_instance(session, ji,
                             usage_str=request.form.get('usage_str', None),
                             wallclock=request.form.get('wallclock', None),
                             maxvmem=request.form.get('maxvmem', None),
                             cpu=request.form.get('cpu', None),
                             io=request.form.get('io', None))
    return "", 200


@app.route('/queue_job', methods=['POST'])
def queue_job():
    logger.debug("Queue Job {}".format(request.form['job_id']))
    with session_scope() as session:
        job = session.query(models.Job)\
            .filter_by(job_id=request.form['job_id']).first()
        job.transition(models.JobStatus.QUEUED_FOR_INSTANTIATION)
    return "", 200


@app.route('/reset_job', methods=['POST'])
def reset_job():
    with session_scope() as session:
        job = session.query(models.Job)\
            .filter_by(job_id=request.form['job_id']).first()
        job.reset()
        session.commit()
    return "", 200


@app.route('/reset_incomplete_jobs', methods=['POST'])
def reset_incomplete_jobs():
    with session_scope() as session:
        up_job = """
            UPDATE job
            SET status=:registered_status, num_attempts=0
            WHERE dag_id=:dag_id
            AND job.status!=:done_status
        """
        up_job_instance = """
            UPDATE job_instance
            JOIN job USING(job_id)
            SET job_instance.status=:error_status
            WHERE job.dag_id=:dag_id
            AND job.status!=:done_status
        """
        log_errors = """
            INSERT INTO job_instance_error_log
                (job_instance_id, description)
            SELECT job_instance_id, 'Job RESET requested' as description
            FROM job_instance
            JOIN job USING(job_id)
            WHERE job.dag_id=:dag_id
            AND job.status!=:done_status
        """
        session.execute(up_job,
                        {"dag_id": request.form['dag_id'],
                         "registered_status": models.JobStatus.REGISTERED,
                         "done_status": models.JobStatus.DONE})
        session.execute(up_job_instance,
                        {"dag_id": request.form['dag_id'],
                         "error_status": models.JobInstanceStatus.ERROR,
                         "done_status": models.JobStatus.DONE})
        session.execute(log_errors,
                        {"dag_id": request.form['dag_id'],
                         "done_status": models.JobStatus.DONE})
        session.commit()
    return "", 200


def _get_job_instance(session, job_instance_id):
    job_instance = session.query(models.JobInstance).filter_by(
        job_instance_id=job_instance_id).first()
    return job_instance


def _update_job_instance_state(session, job_instance, status_id):
    """Advances the states of job_instance and it's associated Job,
    returning any messages that should be published based on
    the transition"""
    logger.debug("Update JI state {} for  {}".format(status_id,
                                                     job_instance))
    job_instance.transition(status_id)
    job = job_instance.job

    # TODO: Investigate moving this publish logic into some SQLAlchemy-
    # event driven framework. Given the amount of code copying here, to
    # ensure consistenty with committed transactions it doesn't feel like
    # the JobStateManager should be the responsible party on this one.
    #
    # ... see tests/tests_job_state_manager.py for Event example
    if job.status in [models.JobStatus.DONE, models.JobStatus.ERROR_FATAL]:
        to_publish = mogrify(job.dag_id, (job.job_id, job.status))
        return to_publish
    else:
        return None


def _update_job_instance(session, job_instance, **kwargs):
    logger.debug("Update JI  {}".format(job_instance))
    for k, v in kwargs.items():
        setattr(job_instance, k, v)
    return
