import sys
import logging
from typing import Optional

from flask import Flask, request
from flask_cors import CORS

from jobmon import config
from jobmon.server.server_config import ServerConfig


def create_app(server_config: Optional[ServerConfig] = None):
    """Create a Flask app"""
    app = Flask(__name__)

    if server_config is None:
        server_config = ServerConfig.from_defaults()
    app.config['SQLALCHEMY_DATABASE_URI'] = server_config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 200}

    # register blueprints
    from jobmon.server.query_server.query_server import jqs
    from jobmon.server.update_server.update_server import jsm
    from jobmon.server.visualization_server.visualization_server import jvs
    app.register_blueprint(jqs)
    app.register_blueprint(jsm)
    app.register_blueprint(jvs)

    # register app with flask-sqlalchemy DB
    from jobmon.models import DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    return app


app = create_app()


@app.before_first_request
def setup_logging():
    # TODO: log level should be configurable via ENV variables. use gunicorn_logger.level
    # gunicorn_logger = logging.getLogger('gunicorn.error')

    # setup console handler
    log_formatter = logging.Formatter(
        '%(asctime)s [%(name)-12s] ' + config.jobmon_version + ' %(module)s %(levelname)-8s '
        '%(threadName)s: %(message)s'
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)

    # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    app_logger = logging.getLogger(app.name)
    app_logger.setLevel(logging.INFO)
    app_logger.addHandler(console_handler)

    # werkzeug logger
    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.disabled = True
    werkzeug_logger.setLevel(logging.ERROR)
    werkzeug_logger.addHandler(console_handler)

    # sqlalchemy logger
    sqlalchemy_logger = logging.getLogger('sqlalchemy')
    sqlalchemy_logger.setLevel(logging.WARNING)
    sqlalchemy_logger.addHandler(console_handler)


@app.before_request
def log_request_info():
    if request.method == "GET":
        app.logger.info(f"URL Path is {request.path}. Args are {request.args}")
    if request.method in ["POST", "PUT"]:
        app.logger.info(f'URL Path is {request.path}. Data is: {request.get_json()}')
