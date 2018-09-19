import os

from flask import Flask
from jobmon.server.services.job_query_server.job_query_server import jqs


def create_app(host=None, conn_str=None):
    """Create a Flask app"""
    app = Flask(__name__)
    if host:
        os.environ['host'] = host
    if conn_str:
        os.environ['conn_str'] = conn_str

    app.register_blueprint(jqs)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        from jobmon.server.database import ScopedSession
        ScopedSession.remove()

    return app


def start(host=None, conn_str=None):
    app = create_app(host, conn_str)
    app.run()
