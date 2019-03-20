import os
from datetime import datetime
from sqlalchemy.orm import contains_eager

from flask import jsonify, request, Blueprint

from jobmon.models import DB
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.job import Job
from jobmon.models.job_instance import JobInstance
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.jobmonLogging import jobmonLogging as logging

try:  # Python 3.5+
    from http import HTTPStatus as StatusCodes
except ImportError:
    try:  # Python 3
        from http import client as StatusCodes
    except ImportError:  # Python 2
        import httplib as StatusCodes

jqs = Blueprint("job_query_server", __name__)

logger = logging.getLogger(__name__)


@jqs.route('/', methods=['GET'])
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


@jqs.route("/time", methods=['GET'])
def get_utc_now():
    logger.debug(logging.myself())
    time = DB.session.execute("select UTC_TIMESTAMP as time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


def get_time(session):
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("session", session))
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


@jqs.route('/workflow/<workflow_id>/workflow_attribute', methods=['GET'])
def get_workflow_attribute(workflow_id):
    """Get a partricular attribute of a particular workflow

    Args:
        workflow_id: id of the workflow to retrieve workflow_attributes for
        workflow_attribute_type: num_age_groups, num_locations, etc.
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("workflow_id", workflow_id))
    workflow_attribute_type = request.args.get('workflow_attribute_type', None)
    logger.debug(workflow_attribute_type)
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
    resp = jsonify(workflow_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow/<workflow_id>/job_attribute', methods=['GET'])
def get_job_attribute_by_workflow(workflow_id):
    """Get a partricular attribute of a particular type of job in the workflow

    Args:
        workflow_id: id of the workflow to retrieve workflow_attributes for
        job_type: type of job getting attributes for
        job_attribute_type: num_locations, wallclock, etc.
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("workflow_id", workflow_id))
    job_attribute_type = request.args.get('job_attribute_type', None)
    logger.debug(job_attribute_type)
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
    logger.debug(attr_dcts)
    resp = jsonify(job_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/job_attribute', methods=['GET'])
def get_job_attribute(job_id):
    """Get a partricular attribute of a particular type of job in the workflow

    Args:
        job_id: id of the job to retrieve job for
        job_attribute_type: num_locations, wallclock, etc.
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("job_id", job_id))
    job_attribute_type = request.args.get('job_attribute_type', None)
    logger.debug(job_attribute_type)
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
    logger.debug(attr_dcts)
    resp = jsonify(job_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/job', methods=['GET'])
def get_jobs_by_status(dag_id):
    """Returns all jobs in the database that have the specified status

    Args:
        status (str): status to query for
        last_sync (datetime): time since when to get jobs
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
    time = get_time(DB.session)
    if request.args.get('status', None) is not None:
        jobs = DB.session.query(Job).filter(
            Job.status == request.args['status'],
            Job.dag_id == dag_id,
            Job.status_date >= last_sync).all()
    else:
        jobs = DB.session.query(Job).filter(
            Job.dag_id == dag_id,
            Job.status_date >= last_sync).all()
    DB.session.commit()
    job_dcts = [j.to_wire() for j in jobs]
    logger.debug(job_dcts)
    resp = jsonify(job_dcts=job_dcts, time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/job_instance', methods=['GET'])
def get_job_instances_by_filter(dag_id):
    """Returns all job_instances in the database that have the specified filter

    Args:
        dag_id (int): dag_id to which the job_instances are attached
        status (list): list of statuses to query for
        runtime (str, optional, option: 'timed_out'): if specified, will only
        return jobs whose runtime is above max_runtime_seconds
    """
    logger.debug(logging.myself())
    logger.debug(logging.logParameter("dag_id", dag_id))
    if request.args.get('runtime', None) is not None:

        instances = DB.session.query(JobInstance). \
            filter_by(dag_id=dag_id).\
            filter(
                JobInstance.status.in_(request.args.getlist('status'))).\
            join(Job).\
            options(contains_eager(JobInstance.job)).\
            filter(Job.max_runtime_seconds != None).all()  # noqa: E711
        DB.session.commit()
        now = datetime.utcnow()
        instances = [r.to_wire() for r in instances
                     if ((now - r.status_date).seconds >
                         r.job.max_runtime_seconds)]
    else:
        instances = DB.session.query(JobInstance). \
            filter_by(dag_id=dag_id).\
            filter(
                JobInstance.status.in_(request.args.getlist('status'))).all()
        DB.session.commit()
        instances = [i.to_wire() for i in instances]
    resp = jsonify(ji_dcts=instances)
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
    logger.debug(dag_ids)
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
    logger.debug(logging.logParameter("dag_id", dag_id))
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
    logger.debug(logging.logParameter("workflow_id", workflow_id))
    wf_run = (DB.session.query(WorkflowRunDAO).filter_by(
        workflow_id=workflow_id,
        status=WorkflowRunStatus.RUNNING,
    ).order_by(WorkflowRunDAO.id.desc()).first())
    DB.session.commit()
    logger.debug(wf_run)
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
    logger.debug(logging.logParameter("workflow_run_id", workflow_run_id))
    jis = DB.session.query(JobInstance).filter_by(
        workflow_run_id=workflow_run_id).all()
    jis = [ji.to_wire() for ji in jis]
    logger.debug(jis)
    DB.session.commit()
    resp = jsonify(job_instances=jis)
    resp.status_code = StatusCodes.OK
    return resp
