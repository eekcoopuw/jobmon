"""Start up the flask services."""
from importlib import import_module
from typing import Any, Dict, List, Optional

from elasticapm.contrib.flask import ElasticAPM
from flask import Flask
import sqlalchemy

from jobmon.configuration import JobmonConfig
from jobmon.server.web import log_config, session_factory
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers


class AppFactory:
    def __init__(
        self,
        sqlalchemy_database_uri: str = "",
        use_logstash: Optional[bool] = None,
        logstash_host: str = "",
        logstash_port: Optional[int] = None,
        logstash_protocol: str = "",
        logstash_log_level: str = "",
        use_apm: Optional[bool] = None,
        apm_server_url: str = "",
        apm_server_name: str = "",
        apm_server_port: Optional[int] = None,
    ) -> None:
        """Initialization of the App Factory."""
        config = JobmonConfig()
        # configuration for sqlalchemy
        if not sqlalchemy_database_uri:
            sqlalchemy_database_uri = config.get("web", "sqlalchemy_database_uri")
        self.engine = sqlalchemy.create_engine(
            sqlalchemy_database_uri, pool_recycle=200, future=True
        )

        # configuration for logstash
        if use_logstash is None:
            use_logstash = config.get_boolean("web", "use_logstash")
        self.use_logstash = use_logstash

        if self.use_logstash and not logstash_host:
            logstash_host = config.get("web", "logstash_host")
        self.logstash_host = logstash_host

        if self.use_logstash and logstash_port is None:
            logstash_port = config.get_int("web", "logstash_port")
        self.logstash_port = logstash_port

        if self.use_logstash and not logstash_protocol:
            logstash_protocol = config.get("web", "logstash_protocol")
        self.logstash_protocol = logstash_protocol

        if self.use_logstash and not logstash_log_level:
            logstash_log_level = config.get("web", "logstash_log_level")
        self.logstash_log_level = logstash_log_level

        if use_apm is None:
            use_apm = config.get_boolean("web", "use_apm")
        self.use_apm = use_apm

        if self.use_apm and not apm_server_url:
            apm_server_url = config.get("web", "apm_server_url")
        self.apm_server_url = apm_server_url

        if self.use_apm and not apm_server_name:
            apm_server_name = config.get("web", "apm_server_name")
        self.apm_server_name = apm_server_name

        if self.use_apm and apm_server_port is None:
            apm_server_port = config.get_int("web", "apm_server_port")
        self.apm_server_port = apm_server_port

        # bind the engine to the session factory before importing the blueprint
        session_factory.configure(bind=self.engine)

        # add logger to app global context
        log_config.configure_logger("jobmon.server.web", self.logstash_handler_config)

        # in memory db must be created in same thread as app
        if str(self.engine.url) == "sqlite://":
            from jobmon.server.web.models import init_db
            init_db(self.engine)

    @property
    def flask_config(self) -> Dict[str, Any]:
        flask_config = {}
        if self.use_apm:
            flask_config["ELASTIC_APM"] = {
                # Set the required service name. Allowed characters:
                # a-z, A-Z, 0-9, -, _, and space
                "SERVICE_NAME": self.apm_server_name,
                # Set the custom APM Server URL (default: http://0.0.0.0:8200)
                "SERVER_URL": f"http://{self.apm_server_url}:{self.apm_server_port}",
                # Set the service environment
                "ENVIRONMENT": "development",
                "DEBUG": True,
            }
        return flask_config

    @property
    def logstash_handler_config(self) -> Optional[Dict[str, Any]]:
        if self.use_logstash:
            logstash_handler_config: Optional[
                Dict
            ] = log_config.get_logstash_handler_config(
                logstash_host=self.logstash_host,
                logstash_port=self.logstash_port,
                logstash_protocol=self.logstash_protocol,
                logstash_log_level=self.logstash_log_level,
            )
        else:
            logstash_handler_config = None
        return logstash_handler_config

    def get_app(
        self, blueprints: List = ["fsm", "cli", "reaper"], url_prefix: str = "/"
    ) -> Flask:
        """Create a Flask app."""
        app = Flask(__name__)
        app.config.from_mapping(self.flask_config)

        if self.use_apm:
            apm = ElasticAPM(app)
        else:
            apm = None

        # register the blueprints we want. they make use of a scoped session attached
        # to the global session factory
        for blueprint in blueprints:
            mod = import_module(f"jobmon.server.web.routes.{blueprint}")
            app.register_blueprint(getattr(mod, "blueprint"), url_prefix=url_prefix)

        # add request logging hooks
        add_hooks_and_handlers(app, apm)

        return app
