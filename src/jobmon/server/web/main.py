"""Initialize Web services."""
from jobmon.server.web.app_factory import AppFactory

app = AppFactory().get_app()
