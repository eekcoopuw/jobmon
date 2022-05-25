"""SQLAlchemy database objects."""
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta


# declarative registry for model elements
Base: DeclarativeMeta = declarative_base()


def init_db(engine):
    """emit DDL for all modules in 'models'"""

    # iterate through the modules in the current package
    package_dir = Path(__file__).resolve().parent
    for (_, module_name, _) in iter_modules([package_dir]):
        import_module(f"{__name__}.{module_name}")

    Base.metadata.create_all(bind=engine)

    # load metadata
    from jobmon.server.web import session_factory
    from jobmon.server.web.models.arg_type import add_arg_types
    from jobmon.server.web.models.workflow_status import add_workflow_statuses
    from jobmon.server.web.models.workflow_run_status import add_workflow_run_statuses
    from jobmon.server.web.models.task_status import add_task_statuses
    from jobmon.server.web.models.task_instance_status import add_task_instance_statuses

    with session_factory(bind=engine) as session:
        add_arg_types(session)
        add_workflow_statuses(session)
        add_workflow_run_statuses(session)
        add_task_statuses(session)
        add_task_instance_statuses(session)
        session.commit()
