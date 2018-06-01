from jobmon.models import Base, JobStatus, JobInstanceStatus
from jobmon.workflow.workflow import WorkflowStatus
from jobmon.workflow.workflow_run import WorkflowRunStatus
from jobmon import database


def create_job_db():
    """create sqlite database from models schema"""
    Base.metadata.create_all(database.engine)  # doesn't create if exists
    return True


def delete_job_db():
    """delete sqlite database from models schema"""
    Base.metadata.drop_all(database.engine)
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
