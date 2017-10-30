from contextlib import contextmanager

import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from jobmon.config import config
from jobmon.models import Base, JobStatus, JobInstanceStatus


if 'sqlite' in config.conn_str:

    # TODO: I've intermittently seen transaction errors when using a
    # sqlite backend. If those continue, investigate these sections of the
    # sqlalchemy docs:
    #
    # http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#sqlite-isolation-level
    # http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#pysqlite-serializable
    #
    # There seem to be some known issues with the pysqlite driver...
    engine = sql.create_engine(config.conn_str,
                               connect_args={'check_same_thread': False},
                               poolclass=StaticPool)
else:
    engine = sql.create_engine(config.conn_str, pool_recycle=300,
                               pool_size=3, max_overflow=100, pool_timeout=120)
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def create_job_db():
    """create sqlite database from models schema"""
    Base.metadata.create_all(engine)  # doesn't create if exists
    return True


def delete_job_db():
    """create sqlite database from models schema"""
    Base.metadata.drop_all(engine)  # doesn't create if exists
    return True


def load_default_statuses(session):
    statuses = []
    for status in ['REGISTERED', 'QUEUED_FOR_INSTANTIATION', 'INSTANTIATED',
                   'RUNNING', 'ERROR_RECOVERABLE', 'ERROR_FATAL', 'DONE']:
        status_obj = JobStatus(id=getattr(JobStatus, status), label=status)
        statuses.append(status_obj)
    for status in ['INSTANTIATED', 'SUBMITTED_TO_BATCH_EXECUTOR', 'RUNNING',
                   'ERROR', 'DONE']:
        status_obj = JobInstanceStatus(id=getattr(JobInstanceStatus, status),
                                       label=status)
        statuses.append(status_obj)
    session.add_all(statuses)


def recreate_engine():
    global engine, Session
    engine = sql.create_engine(config.conn_str, pool_recycle=300,
                               pool_size=3, max_overflow=100, pool_timeout=120)
    Session = sessionmaker(bind=engine)


if __name__ == "__main__":

    create_job_db()

    session = Session()
    try:
        load_default_statuses(session)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
