from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

from sqlalchemy import orm

# sqlalchemy session factory
session_factory = orm.sessionmaker(autocommit=False, autoflush=False, future=True)
# scoped session associated with the current thread
SessionLocal = orm.scoped_session(session_factory)


def init_db(engine):
    from jobmon.server.web.models import Base

    # iterate through the modules in the current package
    package_dir = Path(__file__).resolve().parent / "models"
    for (_, module_name, _) in iter_modules([package_dir]):
        import_module(f"jobmon.server.web.models.{module_name}")

    Base.metadata.create_all(bind=engine)
