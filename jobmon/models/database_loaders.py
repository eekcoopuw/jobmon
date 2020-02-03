from sqlalchemy.exc import IntegrityError

# from jobmon.models.arg import Arg
# from jobmon.models.arg_type import ArgType
# from jobmon.models.command_template_arg_type_mapping import \
#     CommandTemplateArgTypeMapping
# from jobmon.models.executor_parameter_set import ExecutorParameterSet
# from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
# from jobmon.models.task_instance import TaskInstance
from jobmon.models.attributes.task_instance_attribute_type import \
    TaskInstanceAttributeType
# from jobmon.models.tool import Tool
# from jobmon.models.tool_version import ToolVersion
# from jobmon.models.workflow import Workflow
# from jobmon.models.workflow_run import WorkflowRun
# from jobmon.models.workflow_run_status import WorkflowRunStatus
# from jobmon.models.workflow_status import WorkflowStatus


def create_job_db(db):
    """Create sqlite database from models schema"""
    db.create_all()  # doesn't create if exists
    return True


def delete_job_db(db):
    """Delete sqlite database from models schema"""
    db.drop_all()
    return True


def load_attribute_types(db):
    """loads attributes to their specific attribute_type table in db"""
    attribute_types = []

    # load attribute_type and their type for task_instance_attribute_type table
    task_instance_attributes = {'WALLCLOCK': 'string',
                                'CPU': 'string',
                                'IO': 'string',
                                'MAXRSS': 'string',
                                'MAXPSS': 'string',
                                'USAGE_STR': 'string'}
    for attribute in task_instance_attributes:
        task_instance_attribute_types = TaskInstanceAttributeType(
                                name=attribute,
                                type=task_instance_attributes[attribute])
        attribute_types.append(task_instance_attribute_types)

    # add all attribute types to db
    db.session.add_all(attribute_types)


def _truncate(db, model_class):
    db.session.execute("truncate table {t}".
                       format(t=model_class.__tablename__))


def clean_job_db(db):
    """Truncates the mutable tables, does not delete the
    immutable status and type tables. Useful when testing.
    The ordinary user on ephermedb does not have delete priviliges, must use
    root.

    db: flask_sqlalchemy.SQLAlchemy
    Hmm, we have created a dependency on flask. Not good
    """

    # Be careful of the deletion order, must not violate foreign keys.
    # So delete the "leaf" tables first.
    # Must turn off the foreign key checks to allow truncate
    db.session.execute("SET FOREIGN_KEY_CHECKS = 0")
    # _truncate(db, JobInstanceErrorLog)
    # _truncate(db, JobInstance)
    # _truncate(db, JobAttribute)
    # _truncate(db, ExecutorParameterSet)
    # _truncate(db, Job)

    # _truncate(db, WorkflowRunAttribute)
    # _truncate(db, WorkflowRun)
    # _truncate(db, WorkflowAttribute)
    # _truncate(db, Workflow)
    # _truncate(db, TaskDagMeta)
    # _truncate(db, Tool)
    # _truncate(db, ToolVersion)

    # And turn the constraints back on again!
    db.session.execute("SET FOREIGN_KEY_CHECKS = 1")

    db.session.commit()
    return True


def main(db):
    create_job_db(db)

    try:
        # load_default_statuses(db)
        # load_attribute_types(db)
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
