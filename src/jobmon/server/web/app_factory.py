"""Start up the flask services."""
from importlib import import_module
from typing import Any, Dict, Optional

from elasticapm.contrib.flask import ElasticAPM
from flask import Flask

from jobmon.server.web import log_config, session_factory
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.web_config import WebConfig


class AppFactory:

    def __init__(self, web_config: WebConfig):
        self._web_config = web_config

    @property
    def flask_config(self) -> Dict[str, Any]:
        flask_config = {}
        if self._web_config.use_apm:
            url = f"http://{self._web_config.apm_server_url}:{self._web_config.apm_port}"
            flask_config["ELASTIC_APM"] = {
                # Set the required service name. Allowed characters:
                # a-z, A-Z, 0-9, -, _, and space
                "SERVICE_NAME": self._web_config.apm_server_name,
                # Set the custom APM Server URL (default: http://0.0.0.0:8200)
                "SERVER_URL": url,
                # Set the service environment
                "ENVIRONMENT": "development",
                "DEBUG": True,
            }
        return flask_config

    @property
    def logstash_handler_config(self) -> Optional[Dict[str, Any]]:
        if self._web_config.use_logstash:
            logstash_handler_config: Optional[Dict] = log_config.get_logstash_handler_config(
                logstash_host=self._web_config.logstash_host,
                logstash_port=self._web_config.logstash_port,
                logstash_protocol=self._web_config.logstash_protocol,
                logstash_log_level=self._web_config.log_level,
            )
        else:
            logstash_handler_config = None
        return logstash_handler_config

    def create_app_context(self, blueprints=["fsm"]) -> Flask:
        """Create a Flask app."""
        app = Flask(__name__)
        app.config.from_mapping(self.flask_config)

        if self._web_config.use_apm:
            apm = ElasticAPM(app)
        else:
            apm = None

        with app.app_context():

            # bind the engine to the session factory before importing the blueprint
            session_factory.configure(bind=self._web_config.engine)

            # add logger to app global context
            log_config.configure_logger("jobmon.server.web", self.logstash_handler_config)

            # register the blueprints we want. they make use of a scoped session attached
            # to the global session factory
            for blueprint in blueprints:
                mod = import_module(f"jobmon.server.web.routes.{blueprint}")
                app.register_blueprint(getattr(mod, 'blueprint'), url_prefix="/")

            # add request logging hooks
            add_hooks_and_handlers(app, apm)

            return app
