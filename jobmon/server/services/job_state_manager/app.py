import os

from flask import Flask
from jobmon.server.services.job_state_manager.job_state_manager import jsm


def create_app(host, conn_str):
    app = Flask(__name__)
    if host:
        os.environ['host'] = host
    if conn_str:
        os.environ['conn_str'] = conn_str

    app.register_blueprint(jsm)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        from jobmon.server.database import ScopedSession
        ScopedSession.remove()

    return app
