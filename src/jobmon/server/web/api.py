# from jobmon.server.web.start import create_app  # noqa F401
from typing import Optional

from jobmon.server.web.app_factory import AppFactory  # noqa F401
from jobmon.server.web.log_config import configure_logging  # noqa F401
from jobmon.configuration import JobmonConfig


def get_app(config: Optional[JobmonConfig] = None):
    app_factory = AppFactory(config)
    app = app_factory.get_app()
    return app
