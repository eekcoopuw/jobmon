from typing import Optional

from flask import Flask
from flask_cors import CORS

from jobmon.server.web.handlers import add_hooks_and_handlers
from jobmon.server.web.web_config import WebConfig


def create_app(web_config: Optional[WebConfig] = None):
    """Create a Flask app"""
    app = Flask(__name__)

    if web_config is None:
        web_config = WebConfig.from_defaults()
    app.config['SQLALCHEMY_DATABASE_URI'] = web_config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 200}

    # register blueprints
    from jobmon.server.web.jobmon_client.jobmon_client import jobmon_client
    from jobmon.server.web.jobmon_scheduler.jobmon_scheduler import jobmon_scheduler
    from jobmon.server.web.jobmon_swarm.jobmon_swarm import jobmon_swarm
    from jobmon.server.web.jobmon_worker.jobmon_worker import jobmon_worker
    from jobmon.server.web.visualization_server.visualization_server import jvs

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

    # add request logging hooks
    add_hooks_and_handlers(app, web_config.use_rsyslog, web_config.rsyslog_host,
                           web_config.rsyslog_port, web_config.rsyslog_protocol)

    return app
