from jobmon.attributes.attribute_models import WorkflowAttributeType, \
                                               WorkflowRunAttributeType, \
                                               JobAttributeType


def load_attribute_types(session):
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
                           'TAG': 'string'}
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
                               'NUM_SEXES': 'int'}
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
                      'NUM_SEXES': 'int'}
    for attribute in job_attributes:
        job_attribute_types = JobAttributeType(
                                name=attribute,
                                type=job_attributes[attribute])
        attribute_types.append(job_attribute_types)

    # add all attribute types to db
    session.add_all(attribute_types)
    session.commit()
