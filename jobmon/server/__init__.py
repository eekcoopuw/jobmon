import os

from flask import Flask
from flask_cors import CORS

from jobmon.models import DB
from jobmon.server.services.job_query_server.job_query_server import jqs
from jobmon.server.services.job_state_manager.job_state_manager import jsm


def create_app(conn_str):
    """Create a Flask app"""
    app = Flask(__name__)
    if conn_str:
        app.config['SQLALCHEMY_DATABASE_URI'] = conn_str
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['CONN_STR']

    # register blueprints
    app.register_blueprint(jqs)
    app.register_blueprint(jsm)

    # register app with flask-sqlalchemy DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    return app
