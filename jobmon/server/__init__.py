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
    from jobmon.server.jobmon_client.jobmon_client import jobmon_client
    from jobmon.server.jobmon_scheduler.jobmon_scheduler import jobmon_scheduler
    from jobmon.server.jobmon_swarm.jobmon_swarm import jobmon_swarm
    from jobmon.server.jobmon_worker.jobmon_worker import jobmon_worker
    from jobmon.server.visualization_server.visualization_server import jvs

    app.register_blueprint(jobmon_client, url_prefix='/')  # default traffic goes to jobmon_client
    app.register_blueprint(jobmon_client, url_prefix='/client')
    app.register_blueprint(jobmon_scheduler, url_prefix='/scheduler')
    app.register_blueprint(jobmon_swarm, url_prefix='/swarm')
    app.register_blueprint(jobmon_worker, url_prefix='/worker')
    app.register_blueprint(jvs, url_prefix='/viz')

    # register app with flask-sqlalchemy DB
    from jobmon.models import DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    return app


app = create_app()


@app.before_first_request
def setup_logging():
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
