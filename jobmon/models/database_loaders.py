from sqlalchemy.exc import IntegrityError

from jobmon.models.attributes.job_attribute import JobAttribute
from jobmon.models.attributes.job_attribute_type import JobAttributeType
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute
from jobmon.models.attributes.workflow_attribute_type import \
    WorkflowAttributeType
from jobmon.models.attributes.workflow_run_attribute import \
    WorkflowRunAttribute
from jobmon.models.attributes.workflow_run_attribute_type import \
    WorkflowRunAttributeType
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow_status import WorkflowStatus


def create_job_db(db):
    """Create sqlite database from models schema"""
    db.create_all()  # doesn't create if exists
    return True


def delete_job_db(db):
    """Delete sqlite database from models schema"""
    db.drop_all()
    return True


def load_default_statuses(db):
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
    db.session.add_all(statuses)


def load_attribute_types(db):
    """loads attributes to their specific attribute_type table in db"""
    attribute_types = []

    # load attribute_type and their type for workflow_attribute_type table
    workflow_attributes = {'NUM_LOCATIONS': 'int',
                           'NUM_DRAWS': 'int',
                           'NUM_AGE_GROUPS': 'int',
                           'NUM_YEARS': 'int',
                           'NUM_RISKS': 'int',
                           'NUM_CAUSES': 'int',
                           'NUM_SEXES': 'int',
                           'TAG': 'string',
                           'NUM_MEASURES': 'int',
                           'NUM_METRICS': 'int',
                           'NUM_MOST_DETAILED_LOCATIONS': 'int',
                           'NUM_AGGREGATE_LOCATIONS': 'int'}
    for attribute in workflow_attributes:
        workflow_attribute_types = WorkflowAttributeType(
            name=attribute,
            type=workflow_attributes[attribute])
        attribute_types.append(workflow_attribute_types)

    # load attribute_type and their type for workflow_run_attribute_type table
    workflow_run_attributes = {'NUM_LOCATIONS': 'int',
                               'NUM_DRAWS': 'int',
                               'NUM_AGE_GROUPS': 'int',
                               'NUM_YEARS': 'int',
                               'NUM_RISKS': 'int',
                               'NUM_CAUSES': 'int',
                               'NUM_SEXES': 'int',
                               'TAG': 'string',
                               'NUM_MEASURES': 'int',
                               'NUM_METRICS': 'int',
                               'NUM_MOST_DETAILED_LOCATIONS': 'int',
                               'NUM_AGGREGATE_LOCATIONS': 'int',
                               'SLOT_LIMIT_AT_START': 'int',
                               'SLOT_LIMIT_AT_END': 'int'}
    for attribute in workflow_run_attributes:
        workflow_run_attribute_types = WorkflowRunAttributeType(
            name=attribute,
            type=workflow_run_attributes[attribute])
        attribute_types.append(workflow_run_attribute_types)

    # load attribute_type and their type for job_attribute_type table
    job_attributes = {'NUM_LOCATIONS': 'int',
                      'NUM_DRAWS': 'int',
                      'NUM_AGE_GROUPS': 'int',
                      'NUM_YEARS': 'int',
                      'NUM_RISKS': 'int',
                      'NUM_CAUSES': 'int',
                      'NUM_SEXES': 'int',
                      'TAG': 'string',
                      'NUM_MEASURES': 'int',
                      'NUM_METRICS': 'int',
                      'NUM_MOST_DETAILED_LOCATIONS': 'int',
                      'NUM_AGGREGATE_LOCATIONS': 'int',
                      'WALLCLOCK': 'string',
                      'CPU': 'string',
                      'IO': 'string',
                      'MAXRSS': 'string',
                      'USAGE_STR': 'string',
                      }
    for attribute in job_attributes:
        job_attribute_types = JobAttributeType(
                                name=attribute,
                                type=job_attributes[attribute])
        attribute_types.append(job_attribute_types)

    # add all attribute types to db
    db.session.add_all(attribute_types)


def main(db):
    create_job_db(db)

    try:
        load_default_statuses(db)
        load_attribute_types(db)
        db.session.commit()
    except IntegrityError as e:
        db.session.rollback()
        raise IntegrityError("Database is not empty, "
                             "could not create tables {}".format(str(e)))
    except Exception as e:
        db.session.rollback()
        raise
    finally:
        db.session.close()
