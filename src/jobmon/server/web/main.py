"""Initialize Web services."""
from jobmon.server.web import log_config
from jobmon.server.web.app_factory import AppFactory

app_factory = AppFactory()
log_config.configure_logger("jobmon.server.web", app_factory.logstash_handler_config)
app = app_factory.get_app()
