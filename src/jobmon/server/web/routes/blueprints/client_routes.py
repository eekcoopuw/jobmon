"""Routes used by the main jobmon client."""
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import current_app as app, jsonify
from jobmon.server.web.models import DB
from jobmon.server.web.routes import jobmon_client


@jobmon_client.before_request  # try before_first_request so its quicker
def log_request_info() -> None:
    """Add blueprint to logger."""
    app.logger = app.logger.bind(blueprint=jobmon_client.name)
    app.logger.debug("starting route distributor")


@jobmon_client.route('/', methods=['GET'])
def _is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    app.logger.info(f"{os.getpid()}: {app.name} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


def _get_time() -> str:
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    return time


@jobmon_client.route("/time", methods=['GET'])
def get_pst_now() -> Any:
    """Get the time from the database."""
    time = _get_time()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route("/health", methods=['GET'])
def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    app.logger.info(DB.session.bind.pool.status())
    _get_time()
    # Assume that if we got this far without throwing an exception, we should be online
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp


# ############################ TESTING ROUTES ################################################
@jobmon_client.route('/test_bad', methods=['GET'])
def test_bad_route() -> None:
    """Test route to force a 500 error."""
    DB.session.execute('SELECT * FROM blip_bloop_table').all()
