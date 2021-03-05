"""Start up the flask services."""
from typing import Optional

from flask import Flask

from flask_cors import CORS

from jobmon.log_config import get_logstash_handler_config
from jobmon.server.web.handlers import add_hooks_and_handlers
from jobmon.server.web.web_config import WebConfig


def create_app(web_config: Optional[WebConfig] = None):
    """Create a Flask app."""
    app = Flask(__name__)

    if web_config is None:
        web_config = WebConfig.from_defaults()
    if web_config.use_logstash:
        logstash_handler_config = get_logstash_handler_config(
            logstash_host=web_config.logstash_host,
            logstash_port=web_config.logstash_port,
            logstash_protocol=web_config.logstash_protocol
        )
    else:
        logstash_handler_config = None

    app.config['SQLALCHEMY_DATABASE_URI'] = web_config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 200}

    # register blueprints
    from .routes import jobmon_client, jobmon_scheduler, jobmon_swarm, jobmon_worker, jobmon_cli
    # from jobmon.server.web.routes.jobmon_scheduler_2 import jobmon_scheduler
    # from jobmon.server.web.routes.jobmon_swarm_2 import jobmon_swarm
    # from jobmon.server.web.routes.jobmon_worker_2 import jobmon_worker
    # from jobmon.server.web.routes.jobmon_cli_2 import jobmon_cli

    app.register_blueprint(jobmon_client, url_prefix='/')  # default traffic to jobmon_client
    app.register_blueprint(jobmon_client, url_prefix='/client')
    app.register_blueprint(jobmon_scheduler, url_prefix='/scheduler')
    app.register_blueprint(jobmon_swarm, url_prefix='/swarm')
    app.register_blueprint(jobmon_worker, url_prefix='/worker')
    app.register_blueprint(jobmon_cli, url_prefix='/cli')

    # register app with flask-sqlalchemy DB
    from jobmon.server.web.models import DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    # add request logging hooks
    add_hooks_and_handlers(app, logstash_handler_config)

    return app
