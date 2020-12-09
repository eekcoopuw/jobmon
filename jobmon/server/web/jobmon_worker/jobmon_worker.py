from http import HTTPStatus as StatusCodes
import os
from typing import Optional

from flask import jsonify, request, Blueprint, current_app as app
from sqlalchemy.sql import func, text
import sqlalchemy


from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.models.task_instance import TaskInstance
from jobmon.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.models.task_instance import TaskInstanceStatus
from jobmon.server.web.server_side_exception import ServerError

jobmon_worker = Blueprint("jobmon_worker", __name__)


@jobmon_worker.before_request  # try before_first_request so its quicker
def log_request_info():
    app.logger = app.logger.bind(blueprint=jobmon_worker.name)
    app.logger.debug("starting route execution")


@jobmon_worker.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating
    that this responder is in fact listening
    """
    app.logger.info(f"{os.getpid()}: {jobmon_worker.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_worker.route("/time", methods=['GET'])
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


@jobmon_worker.route("/health", methods=['GET'])
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
        resp = jsonify(status='OK')
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/kill_self', methods=['GET'])
def kill_self(task_instance_id: int):
    """Check a task instance's status to see if it needs to kill itself
    (state W, or L)"""
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    kill_statuses = TaskInstance.kill_self_states
    try:
        query = """
            SELECT
                task_instance.id
            FROM
                task_instance
            WHERE
                task_instance.id = :task_instance_id
                AND task_instance.status in :statuses
        """
        should_kill = DB.session.query(TaskInstance).from_statement(text(query)).params(
            task_instance_id=task_instance_id,
            statuses=kill_statuses
        ).one_or_none()
        if should_kill is not None:
            resp = jsonify(should_kill=True)
        else:
            resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/log_running', methods=['POST'])
def log_running(task_instance_id: int):
    """Log a task_instance as running
    Args:

        task_instance_id: id of the task_instance to log as running
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    try:
        data = request.get_json()
        ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
        msg = _update_task_instance_state(ti, TaskInstanceStatus.RUNNING)
        if data.get('executor_id', None) is not None:
            ti.executor_id = data['executor_id']
        if data.get('nodename', None) is not None:
            ti.nodename = data['nodename']
        ti.process_group_id = data['process_group_id']
        ti.report_by_date = func.ADDTIME(
            func.now(), func.SEC_TO_TIME(data['next_report_increment']))
        DB.session.commit()

        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/log_report_by', methods=['POST'])
def log_ti_report_by(task_instance_id: int):
    """Log a task_instance as being responsive with a new report_by_date, this
    is done at the worker node heartbeat_interval rate, so it may not happen at
    the same rate that the reconciler updates batch submitted report_by_dates
    (also because it causes a lot of traffic if all workers are logging report
    _by_dates often compared to if the reconciler runs often)
    Args:

        task_instance_id: id of the task_instance to log
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    try:
        data = request.get_json()
        app.logger.debug(f"Log report_by for TI {task_instance_id}. Data={data}")

        executor_id = data.get('executor_id', None)
        params = {}
        params["next_report_increment"] = data["next_report_increment"]
        params["task_instance_id"] = task_instance_id
        if executor_id is not None:
            params["executor_id"] = executor_id
            query = """
                    UPDATE task_instance
                    SET report_by_date = ADDTIME(
                        CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment)),
                        executor_id = :executor_id
                    WHERE task_instance.id = :task_instance_id"""
        else:
            query = """
                UPDATE task_instance
                SET report_by_date = ADDTIME(
                    CURRENT_TIMESTAMP(), SEC_TO_TIME(:next_report_increment))
                WHERE task_instance.id = :task_instance_id"""

        DB.session.execute(query, params)
        DB.session.commit()
        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/log_usage', methods=['POST'])
def log_usage(task_instance_id: int):
    """Log the usage stats of a task_instance
    Args:

        task_instance_id: id of the task_instance to log done
        usage_str (str, optional): stats such as maxrss, etc
        wallclock (str, optional): wallclock of running job
        maxrss (str, optional): max resident set size mem used
        maxpss (str, optional): max proportional set size mem used
        cpu (str, optional): cpu used
        io (str, optional): io used
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    try:
        data = request.get_json()
        if data.get('maxrss', None) is None:
            data['maxrss'] = '-1'

        app.logger.debug(f"usage_str is {data.get('usage_str', None)}, "
                         f"wallclock is {data.get('wallclock', None)}, "
                         f"maxrss is {data.get('maxrss', None)}, "
                         f"maxpss is {data.get('maxpss', None)}, "
                         f"cpu is {data.get('cpu', None)}, "
                         f" io is {data.get('io', None)}")

        ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
        if data.get('usage_str', None) is not None:
            ti.usage_str = data['usage_str']
        if data.get('wallclock', None) is not None:
            ti.wallclock = data['wallclock']
        if data.get('maxrss', None) is not None:
            ti.maxrss = data['maxrss']
        if data.get('maxpss', None) is not None:
            ti.maxpss = data['maxpss']
        if data.get('cpu', None) is not None:
            ti.cpu = data['cpu']
        if data.get('io', None) is not None:
            ti.io = data['io']
        DB.session.commit()

        resp = jsonify()
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/log_done', methods=['POST'])
def log_done(task_instance_id: int):
    """Log a task_instance as done

    Args:
        task_instance_id: id of the task_instance to log done
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    try:
        data = request.get_json()
        app.logger.debug(f"Log DONE for TI {task_instance_id}. Data: {data}")

        ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
        if data.get('executor_id', None) is not None:
            ti.executor_id = data['executor_id']
        if data.get('nodename', None) is not None:
            ti.nodename = data['nodename']
        msg = _update_task_instance_state(ti, TaskInstanceStatus.DONE)
        DB.session.commit()

        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task_instance/<task_instance_id>/log_error_worker_node', methods=['POST'])
def log_error_worker_node(task_instance_id: int):
    """Log a task_instance as errored
    Args:

        task_instance_id (str): id of the task_instance to log done
        error_message (str): message to log as error
    """
    app.logger = app.logger.bind(task_instance_id=task_instance_id)
    try:
        data = request.get_json()
        error_state = data['error_state']
        error_message = data['error_message']
        executor_id = data.get('executor_id', None)
        nodename = data.get('nodename', None)
        app.logger.debug(f"Log ERROR for TI:{task_instance_id}. Data: {data}")

        ti = DB.session.query(TaskInstance).filter_by(id=task_instance_id).one()
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e

    try:
        resp = _log_error(ti, error_state, error_message, executor_id,
                          nodename)
        return resp
    except sqlalchemy.exc.OperationalError:
        # modify the error message and retry
        new_msg = error_message.encode("latin1", "replace").decode("utf-8")
        resp = _log_error(ti, error_state, new_msg, executor_id, nodename)
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


@jobmon_worker.route('/task/<task_id>/most_recent_ti_error', methods=['GET'])
def get_most_recent_ji_error(task_id: int):
    """
    Route to determine the cause of the most recent task_instance's error
    :param task_id:
    :return: error message
    """
    app.logger = app.logger.bind(task_id=task_id)
    try:
        query = """
            SELECT
                tiel.*
            FROM
                task_instance ti
            JOIN
                task_instance_error_log tiel
                ON ti.id = tiel.task_instance_id
            WHERE
                ti.task_id = :task_id
            ORDER BY
                ti.id desc, tiel.id desc
            LIMIT 1"""
        ti_error = DB.session.query(TaskInstanceErrorLog).from_statement(text(query)).params(
            task_id=task_id
        ).one_or_none()
        DB.session.commit()
        if ti_error is not None:
            resp = jsonify({"error_description": ti_error.description})
        else:
            resp = jsonify({"error_description": ""})
        resp.status_code = StatusCodes.OK
        return resp
    except Exception as e:
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e


# ############################ HELPER FUNCTIONS ###############################
def _update_task_instance_state(task_instance: TaskInstance, status_id: str):
    """Advance the states of task_instance and it's associated Task,
    return any messages that should be published based on
    the transition

    Args:
        task_instance (obj) object of time models.TaskInstance
        status_id (int): id of the status to which to transition
    """
    response = ""
    try:
        task_instance.transition(status_id)
    except InvalidStateTransition:
        if task_instance.status == status_id:
            # It was already in that state, just log it
            msg = f"Attempting to transition to existing state." \
                  f"Not transitioning task, tid= " \
                  f"{task_instance.id} from {task_instance.status} to " \
                  f"{status_id}"
            app.logger.warning(msg)
        else:
            # Tried to move to an illegal state
            msg = f"Illegal state transition. Not transitioning task, " \
                  f"tid={task_instance.id}, from {task_instance.status} to " \
                  f"{status_id}"
            app.logger.error(msg)
    except KillSelfTransition:
        msg = f"kill self, cannot transition tid={task_instance.id}"
        app.logger.warning(msg)
        response = "kill self"
    except Exception as e:
        msg = f"General exception in _update_task_instance_state, " \
              f"jid {task_instance}, transitioning to {task_instance}. " \
              f"Not transitioning task. {e}"
        raise ServerError(f"General exception in _update_task_instance_state, jid "
                          f"{task_instance}, transitioning to {task_instance}. Not "
                          f"transitioning task. Server Error in {request.path}",
                          status_code=500) from e

    return response


def _log_error(ti: TaskInstance, error_state: int, error_msg: str,
               executor_id: Optional[int] = None,
               nodename: Optional[str] = None):
    if nodename is not None:
        ti.nodename = nodename
    if executor_id is not None:
        ti.executor_id = executor_id

    try:
        error = TaskInstanceErrorLog(task_instance_id=ti.id,
                                     description=error_msg)
        DB.session.add(error)
        msg = _update_task_instance_state(ti, error_state)
        DB.session.commit()
        resp = jsonify(message=msg)
        resp.status_code = StatusCodes.OK
    except Exception as e:
        DB.session.rollback()
        app.logger.warning(str(e))
        raise ServerError(f"Unexpected Jobmon Server Error in {request.path}",
                          status_code=500) from e

    return resp
