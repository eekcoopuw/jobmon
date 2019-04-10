from datetime import datetime, timedelta
from http import HTTPStatus as StatusCodes
import json
import os
import socket
import traceback
import warnings

from flask import jsonify, request, Blueprint
from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.attributes.constants import job_attribute
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.attributes.workflow_run_attribute import \
    WorkflowRunAttribute
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow
from jobmon.server.jobmonLogging import jobmonLogging as logging

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
    logger.debug(logging.myself())
    logmsg = "{}: Responder received is_alive?".format(os.getpid())
    logger.debug(logmsg)
    resp = jsonify(msg="Yes, I am alive")
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
        slots: number of slots requested
        num_cores: number of cores requested
        mem_free: number of Gigs of memory requested
        max_attempts: how many times the job should be attempted
        max_runtime_seconds: how long the job should be allowed to run
        context_args: any other args that should be passed to the executor
        tag: job attribute tag
        queue: which queue is being used
        j_resource: if the j_drive is being used
    """
    logger.debug(logging.myself())
    data = request.get_json()
    logger.debug(data)
    job = Job(
        name=data['name'],
        job_hash=data['job_hash'],
        command=data['command'],
        dag_id=data['dag_id'],
        slots=data.get('slots', None),
        num_cores=data.get('num_cores', None),
        mem_free=data.get('mem_free', 2),
        max_attempts=data.get('max_attempts', 1),
        max_runtime_seconds=data.get('max_runtime_seconds', None),
        context_args=data.get('context_args', "{}"),
        tag=data.get('tag', None),
        queue=data.get('queue', None),
        j_resource=data.get('j_resource', False),
        status=JobStatus.REGISTERED)
    DB.session.add(job)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()
    job_dct = job.to_wire()
    resp = jsonify(job_dct=job_dct)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag', methods=['POST'])
def add_task_dag():
    """Add a task_dag to the database

    Args:
        name: name for the task_dag
        user: name of the user of the dag
        dag_hash: unique hash for the task_dag
    """
    logger.debug(logging.myself())
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


def _get_workflow_run_id(job_id):
    """Return the workflow_run_id by job_id"""
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))
    job = DB.session.query(Job).filter_by(job_id=job_id).first()
    logger.debug(logging.logParameter("DB.session", DB.session))
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


def _get_dag_id(job_id):
    """Return the workflow_run_id by job_id"""
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))
    job = DB.session.query(Job).filter_by(job_id=job_id).first()
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()
    return job.dag_id


@jsm.route('/job_instance', methods=['POST'])
def add_job_instance():
    """Add a job_instance to the database

    Args:
        job_id (int): unique id for the job
        executor_type (str): string name of the executor type used
    """
    logger.debug(logging.myself())
    data = request.get_json()
    logger.debug(data)
    logger.debug("Add JI for job {}".format(data['job_id']))
    workflow_run_id = _get_workflow_run_id(data['job_id'])
    dag_id = _get_dag_id(data['job_id'])
    job_instance = JobInstance(
        executor_type=data['executor_type'],
        job_id=data['job_id'],
        dag_id=dag_id,
        workflow_run_id=workflow_run_id)
    DB.session.add(job_instance)
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.commit()
    ji_id = job_instance.job_instance_id

    # TODO: Would prefer putting this in the model, but can't find the
    # right post-create hook. Investigate.
    try:
        job_instance.job.transition(JobStatus.INSTANTIATED)
    except InvalidStateTransition:
        if job_instance.job.status == JobStatus.INSTANTIATED:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   "{}'s job_instance_id {} from I to I"
                   .format(data['job_id'], ji_id))
            warnings.warn(msg)
            logger.debug(msg)
        else:
            raise
    finally:
        DB.session.commit()
    resp = jsonify(job_instance_id=ji_id)
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
    logger.debug(logging.myself())
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
        any other Workflow attributes you want to set
    """
    logger.debug(logging.myself())
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
            filter(WorkflowRunDAO.id == data['wfr_id']).first()
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
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    logger.debug("Log DONE for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.DONE)
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_error', methods=['POST'])
def log_error(job_instance_id):
    """Log a job_instance as errored
    Args:

        job_instance_id (str): id of the job_instance to log done
        error_message (str): message to log as error
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log ERROR for JI {}, message={}".format(
        job_instance_id, data['error_message']))
    ji = _get_job_instance(DB.session, job_instance_id)
    try:
        msg = _update_job_instance_state(ji, JobInstanceStatus.ERROR)
        DB.session.commit()
        error = JobInstanceErrorLog(
            job_instance_id=job_instance_id,
            description=data['error_message'])
        DB.session.add(error)
        logger.debug(logging.logParameter("DB.session", DB.session))
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except InvalidStateTransition:
        log_msg = f"JSM::log_error(), reason={msg}"
        warnings.warn(log_msg)
        logger.debug(log_msg)
        raise
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_executor_id', methods=['POST'])
def log_executor_id(job_instance_id):
    """Log a job_instance's executor id
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log EXECUTOR_ID for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    logger.info("in log_executor_id, ji is {}".format(ji))
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    _update_job_instance(ji, executor_id=data['executor_id'])
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/log_heartbeat', methods=['POST'])
def log_dag_heartbeat(dag_id):
    """Log a job_instance as being responsive, with a heartbeat
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    dag = DB.session.query(TaskDagMeta).filter_by(
        dag_id=dag_id).first()
    if dag:
        # set to database time not web app time
        dag.heartbeat_date = func.UTC_TIMESTAMP()
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance_id/<job_instance_id>/log_report_by', methods=['POST'])
def log_ji_report_by(job_instance_id):
    """Log a job_instance as being responsive, with a heartbeat
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    heartbeat_interval = request.get_json()["heartbeat_interval"]

    # job instance should exist if we are calling this route
    ji = DB.session.query(JobInstance).filter_by(
        job_instance_id=job_instance_id).one()

    # set to database time not web app time
    ji.report_by_date = func.ADDTIME(func.UTC_TIMESTAMP(),
                                     heartbeat_interval)
    DB.session.commit()
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


def _log_ji_error_within_heartbeat(session, job_instance_id, error_msg):
    """log an error if the job instance hasn't been updated since
    reconciliation heartbeat occurred.

    Args:

        session: DB.session or Session object to use to connect to the db
        job_instance_id (int): job_instance_id with which to query the database
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("session", session))
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))

    # query for a job instance if it hasn't been updated since last heartbeat
    ji = session.query(JobInstance).\
        join(TaskDagMeta).\
        filter(JobInstance.dag_id == TaskDagMeta.dag_id).\
        filter(JobInstance.job_instance_id == job_instance_id).\
        filter(JobInstance.status_date <= TaskDagMeta.heartbeat_date)\
        .one_or_none()
    DB.session.commit()

    # only update to error if status hasn't been updated by other process.
    # this avoids race conditions between the reconciliation thread and the
    # worker processes
    if ji:
        try:
            msg = _update_job_instance_state(ji, JobInstanceStatus.ERROR)
            DB.session.commit()
            error = JobInstanceErrorLog(job_instance_id=job_instance_id,
                                        description=error_msg)
            DB.session.add(error)
            DB.session.commit()
        except InvalidStateTransition:
            log_msg = f"JSM::log_error(), reason={msg}"
            warnings.warn(log_msg)
            logger.debug(log_msg)


@jsm.route('/task_dag/<dag_id>/reconcile', methods=['POST'])
def reconcile(dag_id):
    """ensure all jobs that we thought were active continue to log heartbeats
    Args:

        job_instance_id: id of the job_instance to log
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    actual = request.get_json()["executor_ids"]

    # query all job instances that are submitted to executor or running.
    # ignore job instances created after heartbeat began. We'll reconcile them
    # during the next reconciliation loop.
    instances = DB.session.query(JobInstance).\
        join(TaskDagMeta).\
        filter_by(dag_id=dag_id).\
        filter(
            JobInstance.status.in_([
                JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                JobInstanceStatus.RUNNING])).\
        filter(JobInstance.submitted_date <= TaskDagMeta.heartbeat_date).\
        with_entities(JobInstance.job_instance_id, JobInstance.executor_id
                      ).all()
    DB.session.commit()

    for job_instance_id, executor_id in instances:
        if executor_id and executor_id not in actual:
            msg = ("Job no longer visible in qstat, check qacct or jobmon "
                   f"database for executor_id {executor_id} and "
                   f"job_instance_id {job_instance_id}")
            _log_ji_error_within_heartbeat(DB.session, job_instance_id, msg)
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_running', methods=['POST'])
def log_running(job_instance_id):
    """Log a job_instance as running
    Args:

        job_instance_id: id of the job_instance to log as running
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log RUNNING for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
    msg = _update_job_instance_state(ji, JobInstanceStatus.RUNNING)
    ji.nodename = data['nodename']
    ji.process_group_id = data['process_group_id']
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
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    data = request.get_json()
    logger.debug("Log USAGE for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.debug(logging.logParameter("DB.session", DB.session))
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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


@jsm.route('/job/<job_id>/reset', methods=['POST'])
def reset_job(job_id):
    """Reset a job and change its status
    Args:

        job_id: id of the job to reset
    """
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    time = get_time(DB.session)
    up_job = """
        UPDATE job
        SET status=:registered_status, num_attempts=0, status_date='{}'
        WHERE dag_id=:dag_id
        AND job.status!=:done_status
    """.format(time)
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
    logger.debug("Query:\n{}".format(up_job))
    logger.debug(logging.logParameter("DB.session", DB.session))
    DB.session.execute(
        up_job,
        {"dag_id": dag_id,
         "registered_status": JobStatus.REGISTERED,
         "done_status": JobStatus.DONE})
    logger.debug("Query:\n{}".format(up_job_instance))
    DB.session.execute(
        up_job_instance,
        {"dag_id": dag_id,
         "error_status": JobInstanceStatus.ERROR,
         "done_status": JobStatus.DONE})
    logger.debug("Query:\n{}".format(log_errors))
    DB.session.execute(
        log_errors,
        {"dag_id": dag_id,
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
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("session", session))
    logger.debug(logging.logParameter("job_instance_id", job_instance_id))
    job_instance = session.query(JobInstance).filter_by(
        job_instance_id=job_instance_id).first()
    return job_instance


def _update_job_instance_state(job_instance, status_id):
    """Advance the states of job_instance and it's associated Job,
    return any messages that should be published based on
    the transition

    Args:
        job_instance (obj) object of time models.JobInstance
        status_id (int): id of the status to which to transition
    """
    logger.debug(logging.myself())
    logger.debug(f"Update JI state {status_id} for {job_instance}")
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
            print(msg)
        else:
            # Tried to move to an illegal state
            msg = f"Illegal state transition. " \
                f"Not transitioning job, jid= " \
                f"{job_instance.job_instance_id}, " \
                f"from {job_instance.status} to {status_id}"
            # log_and_raise(msg, logger)
            logger.error(msg)
            print(msg)
    except Exception as e:
        msg = f"General exception in _update_job_instance_state, " \
            f"jid {job_instance}, transitioning to {job_instance}. " \
            f"Not transitioning job. {e}"
        log_and_raise(msg, logger)
        print(msg)

    job = job_instance.job

    # TODO: Investigate moving this publish logic into some SQLAlchemy-
    # event driven framework. Given the amount of code copying here, to
    # ensure consistency with committed transactions it doesn't feel like
    # the JobStateManager should be the responsible party on this one.
    #
    # ... see tests/tests_job_state_manager.py for Event example
    if job.status in [JobStatus.DONE, JobStatus.ERROR_FATAL]:
        to_publish = mogrify(job.dag_id, (job.job_id, job.status))
        return to_publish
    else:
        return ""


def _update_job_instance(job_instance, **kwargs):
    """Set attributes on a job_instance, primarily status

    Args:
        job_instance (obj): object of type models.JobInstance
    """
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
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
    logger.debug(logging.myself())
    resp = jsonify({'syslog': logging.isSyslogAttached()})
    return resp


@jsm.route('/debug_on', methods=['POST'])
def setRootLoggerToDebug():
    """
    This function set the root log level to debug. Be careful because you are
    unable to set it back.
    :return:
    """
    logging._setRootLoggerLevel(logging.DEBUG)
    resp = jsonify(msn="The root logger lever has been set to DEBUG. This "
                       "action is irreversible.")
    resp.status_code = StatusCodes.OK
    return resp
