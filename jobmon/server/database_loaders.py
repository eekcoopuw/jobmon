from jobmon.models.sql_base import Base
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.server import database


def create_job_db():
    """Create sqlite database from models schema"""
    Base.metadata.create_all(database.engine)  # doesn't create if exists
    return True


def delete_job_db():
    """Delete sqlite database from models schema"""
    database.ScopedSession.commit()
    database.Session.close_all()
    database.engine.dispose()
    Base.metadata.drop_all(database.engine)
    return True


def load_default_statuses(session):
    """Load all default statuses into the database"""
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
    for status in ['CREATED', 'RUNNING', 'STOPPED', 'ERROR', 'DONE']:
        wfs_obj = WorkflowStatus(id=getattr(WorkflowStatus, status),
                                 label=status)
        statuses.append(wfs_obj)
    for status in ['RUNNING', 'STOPPED', 'ERROR', 'DONE']:
        wfrs_obj = WorkflowRunStatus(id=getattr(WorkflowRunStatus, status),
                                     label=status)
        statuses.append(wfrs_obj)
    session.add_all(statuses)


if __name__ == "__main__":

    create_job_db()

    session = database.Session()
    try:
        load_default_statuses(session)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
