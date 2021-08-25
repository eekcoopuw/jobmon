"""Start up the flask services."""
from typing import Optional

from elasticapm.contrib.flask import ElasticAPM
from flask import Flask
from flask_cors import CORS
from jobmon.log_config import get_logstash_handler_config
from jobmon.server.web.handlers import add_hooks_and_handlers
from jobmon.server.web.web_config import WebConfig


def create_app(web_config: Optional[WebConfig] = None) -> Flask:
    """Create a Flask app."""
    app = Flask(__name__)

    if web_config is None:
        web_config = WebConfig.from_defaults()

    if web_config.use_apm:
        app.config['ELASTIC_APM'] = {
            # Set the required service name. Allowed characters:
            # a-z, A-Z, 0-9, -, _, and space
            'SERVICE_NAME': web_config.apm_server_name,

            # Set the custom APM Server URL (default: http://0.0.0.0:8200)
            'SERVER_URL': f"http://{web_config.apm_server_url}:{web_config.apm_port}",

            # Set the service environment
            'ENVIRONMENT': 'development',

            'DEBUG': True
        }
        ElasticAPM(app)

    if web_config.use_logstash:
        logstash_handler_config = get_logstash_handler_config(
            logstash_host=web_config.logstash_host,
            logstash_port=str(web_config.logstash_port),
            logstash_protocol=web_config.logstash_protocol,
            logstash_log_level=web_config.log_level
        )
    else:
        logstash_handler_config = None

    app.config['SQLALCHEMY_DATABASE_URI'] = web_config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 200}

    # register blueprints
    from jobmon.server.web.routes import finite_state_machine

    # default traffic to jobmon_client
    app.register_blueprint(finite_state_machine, url_prefix='/')

    # register app with flask-sqlalchemy DB
    from jobmon.server.web.models import DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    # add request logging hooks
    add_hooks_and_handlers(app, logstash_handler_config)

    return app
