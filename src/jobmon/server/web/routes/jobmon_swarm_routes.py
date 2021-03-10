"""Routes used by the swarm."""
import os
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify

from jobmon.server.web.models import DB

from . import jobmon_swarm


@jobmon_swarm.before_request  # try before_first_request so its quicker
def log_request_info():
    """Add blueprint to logger."""
    app.logger = app.logger.bind(blueprint=jobmon_swarm.name)
    app.logger.debug("starting route execution")


@jobmon_swarm.route('/', methods=['GET'])
def _is_alive():
    """A simple 'action' that sends a response to the requester indicating that this responder
    is in fact listening.
    """
    app.logger.info(f"{os.getpid()}: {jobmon_swarm.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route("/time", methods=['GET'])
def get_pst_now():
    """Get the current time from the database."""
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route("/health", methods=['GET'])
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
