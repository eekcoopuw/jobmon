from http import HTTPStatus as StatusCodes
import os
import time
from typing import Dict

from flask import jsonify, request, Blueprint
from sqlalchemy.orm import contains_eager
from sqlalchemy.sql import func, text

from jobmon.models import DB
from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_run import WorkflowRun
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.server_logging import jobmonLogging as logging

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
    time = DB.session.execute("SELECT UTC_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


def get_time(session):
    time = session.execute("SELECT UTC_TIMESTAMP AS time").fetchone()['time']
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
        query = """SELECT * 
                           FROM workflow_attribute wf_attr
                           WHERE wf_attr.workflow_id = :workflow_id
                           AND wf_attr.attribute_type = :workflow_attribute_type"""
        attribute = DB.session.query(WorkflowAttribute). \
            from_statement(text(query)). \
            params(workflow_id=workflow_id,
                   workflow_attribute_type=workflow_attribute_type). \
            all()
    else:
        query = """SELECT *
                           FROM workflow_attribute wf_attr
                           WHERE wf_attr.workflow_id = :workflow_id
                           """
        attribute = DB.session.query(WorkflowAttribute) \
            .from_statement(text(query)) \
            .params(workflow_id=workflow_id,
                    workflow_attribute_type=workflow_attribute_type) \
            .all()
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
        job_attribute_type: num_locations, wallclock, etc.
    """
    logger.debug(logging.myself())
    logging.logParameter("workflow_id", workflow_id)
    job_attribute_type = request.args.get('job_attribute_type', None)
    if job_attribute_type:
        query = """SELECT job_attribute.*
                           FROM job
                           JOIN job_attribute
                           ON job.job_id = job_attribute.job_id
                           JOIN workflow wf
                           ON job.dag_id = wf.dag_id
                           WHERE wf.id = :workflow_id
                           AND job_attribute.attribute_type = :job_attribute_type
                           """
        attribute = DB.session.query(JobAttribute) \
            .from_statement(text(query)) \
            .params(workflow_id=workflow_id,
                    job_attribute_type=job_attribute_type) \
            .all()
    else:
        query = """SELECT job_attribute.*
                           FROM job
                           JOIN job_attribute 
                           ON job.job_id = job_attribute.job_id
                           JOIN workflow wf 
                           ON job.dag_id = wf.dag_id
                           WHERE wf.id = :workflow_id
                        """
        attribute = DB.session.query(JobAttribute) \
            .from_statement(text(query)) \
            .params(workflow_id=workflow_id).all()
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
        query = """
                        SELECT *
                        FROM job_attribute
                        WHERE job_attribute.job_id = :job_id
                        AND job_attribute.attribute_type = :job_attribute_type
                        """
        attribute = DB.session.query(JobAttribute) \
            .from_statement(text(query)) \
            .params(job_id=job_id,
                    job_attribute_type=job_attribute_type) \
            .all()
    else:
        query = """
                        SELECT *
                        FROM job_attribute
                        WHERE job_attribute.job_id = :job_id
                        """
        attribute = DB.session.query(JobAttribute) \
            .from_statement(text(query)) \
            .params(job_id=job_id).all()
    DB.session.commit()
    attr_dcts = [j.to_wire() for j in attribute]
    logger.info("job_attr_dct={}".format(attr_dcts))
    resp = jsonify(job_attr_dct=attr_dcts)
    resp.status_code = StatusCodes.OK
    return resp


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
    str_time = get_time(DB.session)
    query = """SELECT *
                   FROM job
                   WHERE dag_id = :dag_id
                   AND status = :job_status_queued
                   AND status_date >= :last_sync
                   ORDER BY job_id
                   LIMIT :n_queued_jobs
                   """
    jobs = DB.session.query(Job).from_statement(text(query)) \
        .params(dag_id=dag_id,
                job_status_queued=JobStatus.QUEUED_FOR_INSTANTIATION,
                last_sync=last_sync, n_queued_jobs=int(n_queued_jobs))
    DB.session.commit()
    job_dcts = [j.to_wire_as_executor_job() for j in jobs]
    resp = jsonify(job_dcts=job_dcts, time=str_time)
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
    str_time = get_time(DB.session)
    if request.args.get('status', None) is not None:
        # select docker.job.job_id, docker.job.job_hash, docker.job.status from
        # docker.job where  docker.job.dag_id=1 and docker.job.status="G";
        # vs.
        # select docker.job.job_id, docker.job.job_hash, docker.job.status from
        # docker.job where  docker.job.status="G" and docker.job.dag_id=1;
        # 0.000 sec vs 0.015 sec (result from MySQL WorkBench)
        # Thus move the dag_id in front of status in the filter
        query = """SELECT job_id, status, job_hash
                           FROM job
                           WHERE dag_id = :dag_id
                           AND status = :status
                           AND status_date >= :last_sync
                        """
        rows = DB.session.query('job_id', 'status', 'job_hash').from_statement(
            text(query)) \
            .params(dag_id=dag_id, status=request.args['status'],
                    last_sync=str(last_sync)).all()
    else:
        query = """SELECT job_id, status, job_hash
                           FROM job
                           WHERE dag_id = :dag_id
                           AND status_date >= :last_sync
                        """
        rows = DB.session.query('job_id', 'status', 'job_hash').from_statement(
            text(query)) \
            .params(dag_id=dag_id, last_sync=str(last_sync)).all()
    DB.session.commit()
    job_dcts = [Job(job_id=row.job_id, status=row.status,
                job_hash=row.job_hash).to_wire_as_swarm_job() for row in rows]
    logger.info("job_attr_dct={}".format(job_dcts))
    resp = jsonify(job_dcts=job_dcts, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/dag/<dag_id>/get_timed_out_executor_ids', methods=['GET'])
def get_timed_out_executor_ids(dag_id):
    """This function isnt used by SGE because it automatically terminates timed
     out jobs, however if an executor is being used that does not automatically
    terminate timed out jobs, do it here. Finds all jobs that have been in the
    submitted or running state for longer than the maximum specified run
    time"""
    query = """SELECT job_instance.job_instance_id, job_instance.executor_id
                   FROM job_instance
                   LEFT JOIN executor_parameter_set
                   ON job_instance.executor_parameter_set_id = executor_parameter_set.id
                   AND executor_parameter_set.max_runtime_seconds != NULL
                   AND TIMEDIFF(UTC_TIMESTAMP(), job_instance.status_date)
                   > SEC_TO_TIME(executor_parameter_set.max_runtime_seconds)
                   AND job_instance.dag_id = :dag_id
                   AND job_instance.status in (:batch, :running)
                   """
    job_instances = DB.session.query(JobInstance) \
        .from_statement(text(query)) \
        .params(dag_id=str(dag_id),
                batch=JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                running=JobInstanceStatus.RUNNING).all()
    jiid_exid_tuples = [(ji.job_instance_id, ji.executor_id) for ji in
                        job_instances]
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
    query = """SELECT *
               FROM job_instance
               WHERE job_instance.dag_id = :dag_id
               AND job_instance.status in (:status_list)
            """
    job_instances = DB.session.query(JobInstance).from_statement(text(query))\
        .params(dag_id=dag_id, status_list=request.args.getlist('status'))\
        .all()
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
    query = """ SELECT job_instance.job_instance_id, job_instance.executor_id
                FROM job_instance 
                JOIN task_dag 
                ON job_instance.dag_id = task_dag.dag_id
                AND job_instance.dag_id = :dag_id
                AND job_instance.status in (:batch, :running)
                AND job_instance.submitted_date <= task_dag.heartbeat_date
                AND job_instance.report_by_date <= UTC_TIMESTAMP()
            """
    rows = DB.session.query('job_instance_id', 'executor_id') \
        .from_statement(text(query)) \
        .params(dag_id=dag_id,
                batch=JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                running=JobInstanceStatus.RUNNING).all()
    DB.session.commit()
    job_instances = [JobInstance(job_instance_id=row.job_instance_id,
                                 executor_id=row.executor_id) for row in rows]
    resp = jsonify(job_instances=[ji.to_wire_as_executor_job_instance()
                                  for ji in job_instances])
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/job_instance_exec_ids',
           methods=['GET'])
def get_job_instance_executor_ids(workflow_run_id):
    """Retrieves all of the executor_ids for the job instances in a given
    workflow run to qdel them to ensure that nothing is running when a new
    workflow run is created"""
    query = """SELECT job_instance.executor_id
               FROM job_instance
               WHERE workflow_run_id = :workflow_run_id
            """
    exec_ids = DB.session.query('executor_id').from_statement(text(query))\
        .params(workflow_run_id=workflow_run_id).all()
    DB.session.commit()
    resp = jsonify(executor_ids=[id[0] for id in exec_ids])
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
        query = """SELECT *
                           FROM task_dag
                           WHERE task_dag.dag_hash = :dag_hash
                        """
        dags = DB.session.query(TaskDagMeta).from_statement(text(query)) \
            .params(dag_hash=request.args.get('dag_hash')).all()
    else:
        dags = DB.session.query(TaskDagMeta).all()
    DB.session.commit()
    dag_ids = [dag.dag_id for dag in dags]
    resp = jsonify(dag_ids=dag_ids)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/workflow/workflow_args', methods=['GET'])
def get_workflow_args():
    """
    Return any dag hashes that are assigned to workflows with identical
    workflow args
    """
    logger.debug(logging.myself())
    workflow_args = request.args['workflow_args']
    query = """SELECT workflow_hash
                   FROM workflow
                   WHERE workflow_args = :workflow_args
                """
    workflow_hashes = DB.session.query('workflow_hash').from_statement(
        text(query)) \
        .params(workflow_args=str(workflow_args)).all()
    DB.session.commit()
    resp = jsonify(workflow_hashes=workflow_hashes)
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
    query = """SELECT *
                   FROM workflow
                   WHERE workflow.dag_id = :dag_id
                   AND workflow_args = :wf_args
                """
    workflow = DB.session.query(Workflow).from_statement(text(query)) \
        .params(dag_id=dag_id,
                wf_args=request.args['workflow_args']).first()
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
    query = """SELECT *
                   FROM workflow_run wf_run
                   WHERE wf_run.workflow_id = :workflow_id
                   AND wf_run.status = :status
                   ORDER BY wf_run.id DESC
                """
    wf_run = DB.session.query(WorkflowRun).from_statement(text(query)) \
        .params(workflow_id=workflow_id,
                status=WorkflowRunStatus.RUNNING).first()
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
    query = """SELECT *
                   FROM job_instance
                   WHERE workflow_run_id=:workflow_run_id
                """
    jis = DB.session.query(JobInstance).from_statement(text(query)) \
        .params(workflow_run_id=workflow_run_id).all()
    jis = [ji.to_wire_as_executor_job_instance() for ji in jis]
    DB.session.commit()
    resp = jsonify(job_instances=jis)
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job_instance/<job_instance_id>/kill_self', methods=['GET'])
def kill_self(job_instance_id):
    """Check a job instance's status to see if it needs to kill itself
    (state W, U or Z)"""
    logger.debug(logging.myself())
    logging.logParameter("job_instance_id", job_instance_id)
    query = f"""SELECT *
                    FROM job_instance
                    WHERE job_instance_id = :job_instance_id
                    AND job_instance.status in 
                    ({str(JobInstance.kill_self_states)[1:-1]})
                """
    should_kill = DB.session.query(JobInstance).from_statement(text(query)) \
        .params(job_instance_id=str(job_instance_id)).first()
    logger.debug(f"Should Kill? {type(should_kill)}, {should_kill}")
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
    query = f"""SELECT m_mem_free, num_cores, max_runtime_seconds 
                FROM job_instance, job 
                WHERE job_instance.job_id=job.job_id AND 
                executor_id = {executor_id}"""
    res = DB.session.execute(query).fetchone()
    DB.session.commit()
    resp = jsonify({'mem': res[0], 'cores': res[1], 'runtime': res[2]})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/most_recent_ji_error', methods=['GET'])
def get_most_recent_ji_error(job_id: int):
    """
    Route to determine the cause of the most recent job_instance's error
    :param job_id:
    :return: error message
    """

    logger.debug(logging.myself())
    logging.logParameter("job_id", job_id)

    query = """
        SELECT
            jiel.*
        FROM
            job_instance ji
        JOIN
            job_instance_error_log jiel
            ON ji.job_instance_id = jiel.job_instance_id
        WHERE
            ji.job_id = :job_id
        ORDER BY
            ji.job_instance_id desc, jiel.id desc
        LIMIT 1"""
    ji_error = DB.session.query(JobInstanceErrorLog).from_statement(
        text(query)).params(job_id=job_id).one_or_none()
    DB.session.commit()
    if ji_error is not None:
        resp = jsonify({"error_description": ji_error.description})
    else:
        resp = jsonify({"error_description": ""})
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
    sql = """SELECT executor_id 
             FROM job_instance 
             WHERE job_instance_id={}""".format(job_instance_id)
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
    sql = """SELECT nodename 
             FROM job_instance 
             WHERE job_instance_id={}""".format(job_instance_id)
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


@jqs.route('/job_instance/<job_instance_id>/get_errors', methods=['GET'])
def get_ji_error(job_instance_id: int):
    """
    This route is created for testing purpose

    :param executor_id:
    :return:
    """
    logger.debug(logging.myself())
    query = f"""SELECT description 
                FROM job_instance_error_log 
                WHERE job_instance_id = {job_instance_id};"""
    result = DB.session.execute(query)
    errors = []
    for r in result:
        errors.append(r[0])
    DB.session.commit()
    logger.debug(errors)
    resp = jsonify({'errors': errors})
    resp.status_code = StatusCodes.OK
    return resp


@jqs.route('/job/<job_id>/status', methods=['GET'])
def get_job_status(job_id: int):
    """
    Route to determine the cause of the most recent job_instance's error
    :param job_id:
    :return: error message
    """

    logger.debug(logging.myself())
    logging.logParameter("job_id", job_id)

    query = "SELECT status FROM job WHERE job_id = {}".format(job_id)
    result = DB.session.execute(query)
    for r in result:
        status = r[0]
    DB.session.commit()
    if status is not None:
        resp = jsonify({"status": status})
        resp.status_code = StatusCodes.OK
    else:
        resp = jsonify({"status": None, "error_msg": "Job not found"})
        resp.status_code = StatusCodes.NO_CONTENT
    return resp


@jqs.route('/workflow_run/<workflow_run_id>/status', methods=['GET'])
def get_workflow_run_status(workflow_run_id: int):
    """
    Route to determine the status of a given workflow
    :param workflow_run_id:
    :return: workflow run status
    """

    logger.debug(logging.myself())
    logging.logParameter("workflow_run_id", workflow_run_id)

    query = "SELECT status " \
            "FROM workflow_run " \
            "WHERE id = :workflow_run_id"
    result = DB.session.query(WorkflowRun).from_statement(text(query))\
        .params(workflow_run_id=workflow_run_id).first()
    DB.session.commit()
    if len(result) > 0:
        resp = jsonify(result[0])
        resp.status_code = StatusCodes.OK
        return resp
    else:
        resp = jsonify("No workflow run found")
        resp.status_code = StatusCodes.NO_CONTENT
        return resp
