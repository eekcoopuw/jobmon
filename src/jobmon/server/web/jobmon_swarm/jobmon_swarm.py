from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import jsonify, request, Blueprint, current_app as app
from sqlalchemy.sql import text


from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.executor_parameter_set import ExecutorParameterSet
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.server_side_exception import ServerError

jobmon_swarm = Blueprint("jobmon_swarm", __name__)


@jobmon_swarm.before_request  # try before_first_request so its quicker
def log_request_info():
    app.logger = app.logger.bind(blueprint=jobmon_swarm.name)
    app.logger.debug("starting route execution")


@jobmon_swarm.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening
    """
    app.logger.info(f"{os.getpid()}: {jobmon_swarm.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route("/time", methods=['GET'])
def get_pst_now():
    try:
        time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
        time = time['time']
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        DB.session.commit()
        resp = jsonify(time=time)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route("/health", methods=['GET'])
def health():
    """
    Test connectivity to the database, return 200 if everything is ok
    Defined in each module with a different route, so it can be checked individually
    """
    try:
        time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
        time = time['time']
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        DB.session.commit()
        # Assume that if we got this far without throwing an exception, we should be online
        resp = jsonify(status='OK')
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route('/workflow/<workflow_id>/task_status_updates', methods=['POST'])
def get_task_by_status_only(workflow_id: int):
    """Returns all tasks in the database that have the specified status

    Args:
        status (str): status to query for
        last_sync (datetime): time since when to get tasks
    """
    app.logger = app.logger.bind(workflow_id=workflow_id)
    try:
        data = request.get_json()

        last_sync = data['last_sync']
        swarm_tasks_tuples = data.get('swarm_tasks_tuples', [])

        # get time from db
        db_time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS t").fetchone()['t']
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

        if swarm_tasks_tuples:
            # Sample swarm_tasks_tuples: [(1, 'I')]
            swarm_task_ids = ",".join([str(task_id[0]) for task_id in swarm_tasks_tuples])
            swarm_tasks_tuples = [(int(task_id), str(status))
                                  for task_id, status in swarm_tasks_tuples]

            query_swarm_tasks_tuples = ""
            for task_id, status in swarm_tasks_tuples:
                query_swarm_tasks_tuples += f"({task_id},'{status}'),"
            # get rid of trailing comma on final line
            query_swarm_tasks_tuples = query_swarm_tasks_tuples[:-1]

            query = """
                SELECT
                    task.id, task.status
                FROM task
                WHERE
                    workflow_id = {workflow_id}
                    AND (
                        (
                            task.id IN ({swarm_task_ids})
                            AND (task.id, status) NOT IN ({tuples})
                        )
                        OR status_date >= '{status_date}')
            """.format(workflow_id=workflow_id,
                       swarm_task_ids=swarm_task_ids,
                       tuples=query_swarm_tasks_tuples,
                       status_date=last_sync)
            app.logger.debug(query)
            rows = DB.session.query(Task).from_statement(text(query)).all()

        else:
            query = """
                SELECT
                    task.id, task.status
                FROM task
                WHERE
                    workflow_id = :workflow_id
                    AND status_date >= :last_sync"""
            rows = DB.session.query(Task).from_statement(text(query)).params(
                workflow_id=workflow_id,
                last_sync=str(last_sync)
            ).all()

        DB.session.commit()
        task_dcts = [row.to_wire_as_swarm_task() for row in rows]
        app.logger.debug("task_dcts={}".format(task_dcts))
        resp = jsonify(task_dcts=task_dcts, time=str_time)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
def terminate_workflow_run(workflow_run_id: int):
    """
    If applicable, moves the workflow run to "T" state, moves the associated workflow to "S".

    Checks if workflow runs that are in "C" or "H" state haven't registered a heartbeat in more
    than the timeout value. If they haven't, the route transitions the workflow and workflow
    run.

    Args:
        workflow_run_id (int): the id of the workflow run to query for
        time_out (int): heatbeat_interval * report_by_buffer, the time to compare the workflow
            runs heartbeat_time against

    Returns:
        resp (Any): whether or not the workflow run was reaped and the response status code
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    try:
        query = """
            SELECT *
            FROM workflow_run
            WHERE
                workflow_run.id = :workflow_run_id
                and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
        """
        wfr = DB.session.query(WorkflowRun).from_statement(text(query)).params(
            workflow_run_id=workflow_run_id
        ).one_or_none()

        try:
            wfr.transition(WorkflowRunStatus.TERMINATED)
            DB.session.commit()
            transitioned = True
        except (InvalidStateTransition, AttributeError):
            # this branch handles race condition or case where no wfr was returned
            transitioned = False

        resp = jsonify(transitioned=transitioned)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route('/task/<task_id>/queue', methods=['POST'])
def queue_job(task_id: int):
    """Queue a job and change its status
    Args:

        job_id: id of the job to queue
    """
    app.logger = app.logger.bind(task_id=task_id)
    try:
        task = DB.session.query(Task).filter_by(id=task_id).one()
        try:
            task.transition(TaskStatus.QUEUED_FOR_INSTANTIATION)
        except InvalidStateTransition:
            # Handles race condition if the task has already been queued
            if task.status == TaskStatus.QUEUED_FOR_INSTANTIATION:
                msg = ("Caught InvalidStateTransition. Not transitioning job "
                       f"{task_id} from Q to Q")
                app.logger.warning(msg)
            else:
                raise
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/update_status', methods=['PUT'])
def log_workflow_run_status_update(workflow_run_id: int):
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    try:
        data = request.get_json()
        app.logger.debug(f"Log status update for workflow_run_id:{workflow_run_id}."
                         f"Data: {data}")

        workflow_run = DB.session.query(WorkflowRun).filter_by(id=workflow_run_id).one()
        workflow_run.transition(data["status"])
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


def get_time(session):
    time = session.execute("select CURRENT_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/aborted/<aborted_seconds>',
                    methods=['PUT'])
def get_run_status_and_latest_task(workflow_run_id: int, aborted_seconds: int):
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    try:
        # If the last task was more than 2 minutes ago, transition wfr to A state
        # Also check WorkflowRun status_date to avoid possible race condition where reaper
        # checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
        # while waiting for a resume (when workflow is in suspended state)
        query = """
            SELECT
                workflow_run.*,
                TIMESTAMPDIFF(
                    SECOND, workflow_run.status_date, CURRENT_TIMESTAMP
                ) AS workflow_created,
                TIMESTAMPDIFF(
                    SECOND, max(task.status_date), CURRENT_TIMESTAMP
                ) AS task_created
            FROM workflow_run
            JOIN workflow ON workflow_run.workflow_id = workflow.id
            LEFT JOIN task ON workflow_run.workflow_id = task.workflow_id
            WHERE
                workflow_run.id = :workflow_run_id
                AND workflow.status != 'S'
            HAVING
                (
                    workflow_created > :aborted_seconds
                    AND task_created > :aborted_seconds
                )
                OR (workflow_created > :aborted_seconds and task_created is NULL)
        """
        wfr = DB.session.query(WorkflowRun).from_statement(text(query)).params(
            workflow_run_id=workflow_run_id, aborted_seconds=aborted_seconds
        ).one_or_none()
        DB.session.commit()

        if wfr is not None:
            wfr.transition(WorkflowRunStatus.ABORTED)
            DB.session.commit()
            aborted = True
        else:
            aborted = False

        resp = jsonify(was_aborted=aborted)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_swarm.route('/workflow_run/<workflow_run_id>/log_heartbeat', methods=['POST'])
def log_wfr_heartbeat(workflow_run_id: int):
    """Log a workflow_run as being responsive, with a heartbeat
    Args:

        workflow_run_id: id of the workflow_run to log
    """
    app.logger = app.logger.bind(workflow_run_id=workflow_run_id)
    try:
        params = {"workflow_run_id": int(workflow_run_id)}
        query = """
            UPDATE workflow_run
            SET heartbeat_date = CURRENT_TIMESTAMP()
            WHERE id = :workflow_run_id
        """
        DB.session.execute(query, params)
        DB.session.commit()
        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


def _transform_mem_to_gb(mem_str: Any) -> float:
    # we allow both upper and lowercase g, m, t options
    # BUG g and G are not the same
    if mem_str is None:
        return 2
    if type(mem_str) in (float, int):
        return mem_str
    if mem_str[-1].lower() == "m":
        mem = float(mem_str[:-1])
        mem /= 1000
    elif mem_str[-2:].lower() == "mb":
        mem = float(mem_str[:-2])
        mem /= 1000
    elif mem_str[-1].lower() == "t":
        mem = float(mem_str[:-1])
        mem *= 1000
    elif mem_str[-2:].lower() == "tb":
        mem = float(mem_str[:-2])
        mem *= 1000
    elif mem_str[-1].lower() == "g":
        mem = float(mem_str[:-1])
    elif mem_str[-2:].lower() == "gb":
        mem = float(mem_str[:-2])
    else:
        mem = 1
    return mem


@jobmon_swarm.route('/task/<task_id>/update_resources', methods=['POST'])
def update_task_resources(task_id: int):
    """ Change the resources set for a given task

    Args:
        task_id (int): id of the task for which resources will be changed
        parameter_set_type (str): parameter set type for this task
        max_runtime_seconds (int, optional): amount of time task is allowed to
            run for
        context_args (dict, optional): unstructured parameters to pass to
            executor
        queue (str, optional): sge queue to submit tasks to
        num_cores (int, optional): how many cores to get from sge
        m_mem_free ():
        j_resource (bool, optional): whether to request access to the j drive
        resource_scales (dict): values to scale by upon resource error
        hard_limit (bool): whether to move queues if requester resources exceed
            queue limits
    """
    app.logger = app.logger.bind(task_id=task_id)
    try:
        data = request.get_json()
        parameter_set_type = data["parameter_set_type"]

        try:
            task_id = int(task_id)
        except ValueError:
            resp = jsonify(msg="task_id {} is not a number".format(task_id))
            resp.status_code = StatusCodes.INTERNAL_SERVER_ERROR
            return resp

        exec_params = ExecutorParameterSet(
            task_id=task_id,
            parameter_set_type=parameter_set_type,
            max_runtime_seconds=data.get('max_runtime_seconds', None),
            context_args=data.get('context_args', None),
            queue=data.get('queue', None),
            num_cores=data.get('num_cores', None),
            m_mem_free=_transform_mem_to_gb(data.get("m_mem_free")),
            j_resource=data.get('j_resource', False),
            resource_scales=data.get('resource_scales', None),
            hard_limits=data.get('hard_limits', False))
        DB.session.add(exec_params)
        DB.session.flush()  # get auto increment
        exec_params.activate()
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e
