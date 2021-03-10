"""Routes for Scheduler component of client architecture."""
import os
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify

from jobmon.server.web.models import DB

from . import jobmon_scheduler


@jobmon_scheduler.before_request
def log_request_info():
    """Add blueprint to logger."""
    app.logger = app.logger.bind(blueprint=jobmon_scheduler.name)
    app.logger.debug("starting route execution")


@jobmon_scheduler.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating that this responder
    is in fact listening.
    """
    app.logger.info(f"{os.getpid()}: {__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route("/time", methods=['GET'])
def get_pst_now():
    """Get the current time according to the database."""
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_scheduler.route("/health", methods=['GET'])
def health():
    """
    Test connectivity to the database, return 200 if everything is ok
    Defined in each module with a different route, so it can be checked individually
    """
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    # Assume that if we got this far without throwing an exception, we should be online
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp
