"""The subpackages jobmon_* are deployed as individual services in kubernetes."""
from sqlalchemy import orm

# configurable session factory. add an engine using session_factory.configure(bind=eng)
session_factory = orm.sessionmaker(autocommit=False, autoflush=False, future=True)
