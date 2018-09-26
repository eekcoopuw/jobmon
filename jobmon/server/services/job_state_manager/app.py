import os

from flask import Flask
from flask_cors import CORS
from jobmon.server.services.job_state_manager.job_state_manager import jsm


def create_app(host=None, conn_str=None):
    """Create a Flask app"""
    app = Flask(__name__)
    if host:
        os.environ['HOST'] = host
    if conn_str:
        os.environ['CONN_STR'] = conn_str

    app.register_blueprint(jsm)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        from jobmon.server.database import ScopedSession
        ScopedSession.remove()

    return app


def start(host=None, conn_str=None):
    app = create_app(host, conn_str)
    app.run(host='0.0.0.0', debug=True, port=80)


app = create_app()
CORS(app)

