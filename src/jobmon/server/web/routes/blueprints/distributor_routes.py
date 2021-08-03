"""Routes for Distributor component of client architecture."""
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import current_app as app, jsonify
from jobmon.server.web.models import DB
from jobmon.server.web.routes import jobmon_distributor


@jobmon_distributor.before_request
def log_request_info() -> None:
    """Add blueprint to logger."""
    app.logger = app.logger.bind(blueprint=jobmon_distributor.name)
    app.logger.debug("starting route distributor")


@jobmon_distributor.route('/', methods=['GET'])
def _is_alive() -> Any:
    """Send a response to the requester indicating that this responder is listening."""
    app.logger.info(f"{os.getpid()}: {__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_distributor.route("/time", methods=['GET'])
def get_pst_now() -> Any:
    """Get the current time according to the database."""
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_distributor.route("/health", methods=['GET'])
def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    # Assume that if we got this far without throwing an exception, we should be online
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp
