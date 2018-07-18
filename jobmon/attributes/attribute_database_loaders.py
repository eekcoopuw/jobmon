from jobmon.attributes.attribute_models import WorkflowAttributeType
from jobmon.attributes.constants import workflow_attribute_type


def load_attribute_types(session):
    """adds list of attributes to a specific attribute_type table"""
    attribute_types = []

    # load attributes for workflow_attribute_types table
    for attribute_type in workflow_attribute_type:
        wf_attribute_types = WorkflowAttributeType(name=attribute_type,
                                                    type=getattr(workflow_attribute_type, attribute_type).TYPE)
        attribute_types.append(wf_attribute_types)

    session.add_all(attribute_types)
    session.commit()