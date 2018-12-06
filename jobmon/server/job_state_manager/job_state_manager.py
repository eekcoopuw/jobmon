import logging
import os
import json
from datetime import datetime
from flask import jsonify, request, Blueprint
import warnings

from jobmon.models import DB
# TODO from jobmon.models.attributes import attribute_models
from jobmon.models.attributes.constants import job_attribute
from jobmon.models.job import Job
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.job_status import JobStatus
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow

try:  # Python 3.5+
    from http import HTTPStatus as StatusCodes
except ImportError:
    try:  # Python 3
        from http import client as StatusCodes
    except ImportError:  # Python 2
        import httplib as StatusCodes

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
    data = request.get_json()
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
    data = request.get_json(force=True)
    dag = TaskDagMeta(
        name=data['name'],
        user=data['user'],
        dag_hash=data['dag_hash'])
    DB.session.add(dag)
    DB.session.commit()
    dag_id = dag.dag_id
    resp = jsonify(dag_id=dag_id)
    resp.status_code = StatusCodes.OK
    return resp


def _get_workflow_run_id(job_id):
    """Return the workflow_run_id by job_id"""
    job = DB.session.query(Job).filter_by(job_id=job_id).first()
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
    data = request.get_json()
    logger.debug("Add JI for job {}".format(data['job_id']))
    workflow_run_id = _get_workflow_run_id(data['job_id'])
    job_instance = JobInstance(
        executor_type=data['executor_type'],
        job_id=data['job_id'],
        workflow_run_id=workflow_run_id)
    DB.session.add(job_instance)
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
    data = request.get_json()
    if request.method == 'POST':
        wf = Workflow(dag_id=data['dag_id'],
                      workflow_args=data['workflow_args'],
                      workflow_hash=data['workflow_hash'],
                      name=data['name'],
                      user=data['user'],
                      description=data.get('description', ""))
        DB.session.add(wf)
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
        stdout (str): where stdout should be directedf
        project (str): sge project where this workflow_run should be run
        slack_channel (str): channel where this workflow_run should send
        notifications
        any other Workflow attributes you want to set
    """
    data = request.get_json()
    if request.method == 'POST':
        wfr = WorkflowRunDAO(workflow_id=data['workflow_id'],
                             user=data['user'],
                             hostname=data['hostname'],
                             pid=data['pid'],
                             stderr=data['stderr'],
                             stdout=data['stdout'],
                             project=data['project'],
                             slack_channel=data['slack_channel'])
        workflow = DB.session.query(Workflow).\
            filter(Workflow.id == data['workflow_id']).first()
        # Set all previous runs to STOPPED
        for run in workflow.workflow_runs:
            run.status = WorkflowRunStatus.STOPPED
        DB.session.add(wfr)
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
    """Log a job_istnace as done
    Args:

        job_instance_id: id of the job_instance to log done
    """
    logger.debug("Log DONE for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
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
    data = request.get_json()
    logger.debug("Log ERROR for JI {}, message={}".format(
        job_instance_id, data['error_message']))
    ji = _get_job_instance(DB.session, job_instance_id)
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.ERROR)
    DB.session.commit()
    error = JobInstanceErrorLog(
        job_instance_id=job_instance_id,
        description=data['error_message'])
    DB.session.add(error)
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_instance/<job_instance_id>/log_executor_id', methods=['POST'])
def log_executor_id(job_instance_id):
    """Log a job_instance's executor id
    Args:

        job_instance_id: id of the job_instance to log
    """
    data = request.get_json()
    logger.debug("Log EXECUTOR_ID for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    logger.info("in log_executor_id, ji is {}".format(ji))
    msg = _update_job_instance_state(
        ji, JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    _update_job_instance(ji, executor_id=data['executor_id'])
    DB.session.commit()
    resp = jsonify(message=msg)
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/task_dag/<dag_id>/log_heartbeat', methods=['POST'])
def log_heartbeat(dag_id):
    """Log a job_instance as being responsive, with a heartbeat
    Args:

        job_instance_id: id of the job_instance to log
    """
    dag = DB.session.query(TaskDagMeta).filter_by(
        dag_id=dag_id).first()
    if dag:
        dag.heartbeat_date = datetime.utcnow()
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
    data = request.get_json()
    logger.debug("Log RUNNING for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
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
    data = request.get_json()
    logger.debug("Log USAGE for JI {}".format(job_instance_id))
    ji = _get_job_instance(DB.session, job_instance_id)
    msg = _update_job_instance(ji, nodename=data['nodename'])
    DB.session.commit()
    resp = jsonify(message=msg)
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
    data = request.get_json()
    logger.debug("Log USAGE for JI {}".format(job_instance_id))
    if data.get('maxrss', None) is None:
        data['maxrss'] = '-1'

    keys_to_attrs = {data.get('wallclock', None): job_attribute.WALLCLOCK,
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
    job_id = job_instance.job_id
    msg = _update_job_instance(job_instance,
                               usage_str=data.get('usage_str', None),
                               wallclock=data.get('wallclock', None),
                               maxrss=data.get('maxrss', None),
                               cpu=data.get('cpu', None),
                               io=data.get('io', None))
    for k in keys_to_attrs:
        logger.debug(
            'The value of {kval} being set in the attribute table  is {k}'.
            format(kval=keys_to_attrs[k], k=k))
        if k is not None:
            ja = (attribute_models.JobAttribute(
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
    logger.debug("Queue Job {}".format(job_id))
    job = DB.session.query(Job)\
        .filter_by(job_id=job_id).first()
    try:
        job.transition(JobStatus.QUEUED_FOR_INSTANTIATION)
    except InvalidStateTransition:
        if job.status == JobStatus.QUEUED_FOR_INSTANTIATION:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   "{} from Q to Q".format(job_id))
            warnings.warn(msg)
            logger.debug(msg)
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
    job = DB.session.query(Job).filter_by(job_id=job_id).first()
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
    DB.session.execute(
        up_job,
        {"dag_id": dag_id,
         "registered_status": JobStatus.REGISTERED,
         "done_status": JobStatus.DONE})
    DB.session.execute(
        up_job_instance,
        {"dag_id": dag_id,
         "error_status": JobInstanceStatus.ERROR,
         "done_status": JobStatus.DONE})
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
    logger.debug("Update JI state {} for  {}".format(status_id,
                                                     job_instance))
    try:
        job_instance.transition(status_id)
    except InvalidStateTransition:
        if job_instance.status == status_id:
            msg = ("Caught InvalidStateTransition. Not transitioning job "
                   "{} from {} to {}".format(job_instance.job_instance_id,
                                             job_instance.status, status_id))
            warnings.warn(msg)
            logger.debug(msg)
        else:
            raise
    job = job_instance.job

    # TODO: Investigate moving this publish logic into some SQLAlchemy-
    # event driven framework. Given the amount of code copying here, to
    # ensure consistenty with committed transactions it doesn't feel like
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
    logger.debug("Update JI  {}".format(job_instance))
    status_requested = kwargs.get('status', None)
    if status_requested is not None:
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
        workflow_id (int): id of the workflow on which to set attributres
        attribute_type (obj): object of type WorkflowAttribute
        value (str): value of the WorkflowAttribute to add
    """
    data = request.get_json()
    workflow_attribute = attribute_models.WorkflowAttribute(
        workflow_id=data['workflow_id'],
        attribute_type=data['attribute_type'],
        value=data['value'])
    DB.session.add(workflow_attribute)
    DB.session.commit()
    resp = jsonify({'workflow_attribute_id': workflow_attribute.id})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/workflow_run_attribute', methods=['POST'])
def add_workflow_run_attribute():
    """Set attributes on a workflow_run

    Args:
        workflow_run)_id (int): id of the workflow_run on which to set
        attributres
        attribute_type (obj): object of type WorkflowRunAttribute
        value (str): value of the WorkflowRunAttribute to add
    """
    data = request.get_json()
    workflow_run_attribute = attribute_models.\
        WorkflowRunAttribute(workflow_run_id=data['workflow_run_id'],
                             attribute_type=data['attribute_type'],
                             value=data['value'])
    DB.session.add(workflow_run_attribute)
    DB.session.commit()
    resp = jsonify({'workflow_run_attribute_id': workflow_run_attribute.id})
    resp.status_code = StatusCodes.OK
    return resp


@jsm.route('/job_attribute', methods=['POST'])
def add_job_attribute():
    """Set attributes on a job

    Args:
        job_id (int): id of the job on which to set attributres
        attribute_type (obj): object of type JobAttribute
        value (str): value of the JobAttribute to add
    """
    data = request.get_json()
    job_attribute = attribute_models.\
        JobAttribute(job_id=data['job_id'],
                     attribute_type=data['attribute_type'],
                     value=data['value'])
    DB.session.add(job_attribute)
    DB.session.commit()
    resp = jsonify({'job_attribute_id': job_attribute.id})
    resp.status_code = StatusCodes.OK
    return resp
