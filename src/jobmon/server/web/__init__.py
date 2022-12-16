"""The subpackages jobmon_* are deployed as individual services in kubernetes."""
from sqlalchemy import orm
from jobmon.server.web.log_config import configure_structlog

configure_structlog()

# configurable session factory. add an engine using session_factory.configure(bind=eng)
session_factory = orm.sessionmaker(future=True)
