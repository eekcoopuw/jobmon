"""Routes for CLI requests."""
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify

from jobmon.server.web.models import DB

from werkzeug.local import LocalProxy

from . import jobmon_cli

logger = LocalProxy(lambda: app.logger)


@jobmon_cli.route("/health", methods=['GET'])
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
