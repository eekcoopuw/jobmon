import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker

from jobmon import config
from jobmon.models import Base, JobStatus, JobInstanceStatus

engine = sql.create_engine(config.conn_str, pool_recycle=300, pool_size=3,
                           max_overflow=100, pool_timeout=120)
Session = sessionmaker(bind=engine)


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
    for status in ['INSTANTIATED', 'RUNNING', 'ERROR', 'DONE']:
        status_obj = JobInstanceStatus(id=getattr(JobInstanceStatus, status),
                                       label=status)
        statuses.append(status_obj)
    session.add_all(statuses)


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
