"""SQLAlchemy database objects."""
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

from sqlalchemy.orm import declarative_base


# declarative registry for model elements
Base = declarative_base()


def init_db(engine):
    """emit DDL for all modules in 'models'"""

    # iterate through the modules in the current package
    package_dir = Path(__file__).resolve().parent / "models"
    for (_, module_name, _) in iter_modules([package_dir]):
        import_module(f"jobmon.server.web.models.{module_name}")

    Base.metadata.create_all(bind=engine)
