"""SQLAlchemy database objects."""
import re
from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules
from typing import Any

from sqlalchemy import CheckConstraint, create_engine, event, func, String, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
import structlog


logger = structlog.get_logger(__name__)

# declarative registry for model elements
Base: DeclarativeMeta = declarative_base()


@event.listens_for(Base, "instrument_class", propagate=True)
def add_string_length_constraint(Base: DeclarativeMeta, cls_: Any) -> None:
    """Add check constraint to enforce column size limits on sqlite."""
    table = cls_.__table__

    for column in table.columns:
        if isinstance(column.type, String):
            length = column.type.length

            if length is not None:
                logger.debug(
                    f"adding check constraint to {table}.{column} of len={length}"
                )
                CheckConstraint(
                    func.length(column) <= length,
                    table=column,
                )


def load_model() -> None:
    """Iterate through the modules in the current package."""
    package_dir = Path(__file__).resolve().parent
    for (_, module_name, _) in iter_modules([str(package_dir)]):
        import_module(f"{__name__}.{module_name}")


def init_db(engine: Engine) -> None:
    """Emit DDL for all modules in 'models'."""
    emit_ddl = True

    # dialect specific init logic
    if engine.dialect.name == "mysql":
        event.remove(Base, "instrument_class", add_string_length_constraint)

        if not engine.url.database:
            raise ValueError("Engine url must include database when calling init_db.")

        # create schema if not exists
        try:
            # will fail to connect if database doesn't exist, raising OperationalError
            with engine.connect() as conn:
                emit_ddl = False
        except OperationalError:
            # strip database
            no_schema_engine = create_engine(
                str(engine.url).replace(engine.url.database, "")
            )
            create_db_query = f"CREATE DATABASE {engine.url.database}"
            with no_schema_engine.connect() as conn:
                conn.execute(text(create_db_query))

    if emit_ddl:
        load_model()

        Base.metadata.create_all(bind=engine)

        # load metadata
        from jobmon.server.web import session_factory
        from jobmon.server.web.models.arg_type import add_arg_types
        from jobmon.server.web.models.cluster_type import add_cluster_types
        from jobmon.server.web.models.cluster import add_clusters
        from jobmon.server.web.models.queue import add_queues
        from jobmon.server.web.models.task_resources_type import (
            add_task_resources_types,
        )
        from jobmon.server.web.models.task_status import add_task_statuses
        from jobmon.server.web.models.task_instance_status import (
            add_task_instance_statuses,
        )
        from jobmon.server.web.models.workflow_status import add_workflow_statuses
        from jobmon.server.web.models.workflow_run_status import (
            add_workflow_run_statuses,
        )

        with session_factory(bind=engine) as session:
            metadata_loaders = [
                add_arg_types,
                add_cluster_types,
                add_clusters,
                add_queues,
                add_task_resources_types,
                add_task_statuses,
                add_task_instance_statuses,
                add_workflow_statuses,
                add_workflow_run_statuses,
            ]
            for loader in metadata_loaders:
                loader(session)
                session.flush()
            session.commit()


def term_db(engine: Engine) -> None:
    """Terminate/drop a dev database."""

    # dialect specific init logic
    if engine.dialect.name == "mysql":
        if not re.match("jobmon_[0-9]+", engine.url.database):
            raise ValueError("To-be-termed database must be a TAD dev database, "
                             "whose name is in such a format as jobmon_123.")
        with engine.connect() as conn:
            drop_db_query = f"DROP DATABASE {engine.url.database}"
            conn.execute(text(drop_db_query))


