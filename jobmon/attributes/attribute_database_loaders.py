from jobmon.attributes.attribute_models import WorkflowAttributeType


def load_attribute_types(session):
    """adds list of attributes to a specific attribute_type table"""
    attribute_types = []

    # load attribute_type and their type for workflow_attribute_type table
    workflow_attribute_types = {'NUM_LOCATIONS': 'int',
                                'NUM_DRAWS': 'int',
                                'NUM_AGE_GROUPS': 'int',
                                'NUM_YEARS': 'int',
                                'NUM_RISKS': 'int',
                                'NUM_CAUSES': 'int',
                                'NUM_SEXES': 'int',
                                'TAG': 'string'}
    for attribute_type in workflow_attribute_types:
        wf_attributes = WorkflowAttributeType(name=attribute_type,
                                              type=workflow_attribute_types[attribute_type])
        attribute_types.append(wf_attributes)

    session.add_all(attribute_types)
    session.commit()
