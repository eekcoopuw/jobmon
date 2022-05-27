"""SQLAlchemy database objects."""
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta


# declarative registry for model elements
Base: DeclarativeMeta = declarative_base()


def load_model():
    # iterate through the modules in the current package
    package_dir = Path(__file__).resolve().parent
    for (_, module_name, _) in iter_modules([package_dir]):
        import_module(f"{__name__}.{module_name}")


def init_db(engine):
    """emit DDL for all modules in 'models'"""
    load_model()

    Base.metadata.create_all(bind=engine)

    # load metadata
    from jobmon.server.web import session_factory
    from jobmon.server.web.models.arg_type import add_arg_types
    from jobmon.server.web.models.cluster_type import add_cluster_types
    from jobmon.server.web.models.cluster import add_clusters
    from jobmon.server.web.models.queue import add_queues
    from jobmon.server.web.models.task_status import add_task_statuses
    from jobmon.server.web.models.task_instance_status import add_task_instance_statuses
    from jobmon.server.web.models.workflow_status import add_workflow_statuses
    from jobmon.server.web.models.workflow_run_status import add_workflow_run_statuses

    with session_factory(bind=engine) as session:
        metadata_loaders = [
            add_arg_types,
            add_cluster_types,
            add_clusters,
            add_queues,
            add_task_statuses,
            add_task_instance_statuses,
            add_workflow_statuses,
            add_workflow_run_statuses
        ]
        for loader in metadata_loaders:
            loader(session)
            session.flush()
        session.commit()
