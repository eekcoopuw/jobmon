import os

from flask import Flask
from jobmon.server.services.job_query_server.job_query_server import jqs


def create_app(host=None, conn_str=None):
    app = Flask(__name__)
    if host:
        os.environ['host'] = host
    if conn_str:
        os.environ['conn_str'] = conn_str

    app.register_blueprint(jqs)

    return app
