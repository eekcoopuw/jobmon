from jobmon.attributes.attribute_models import WorkflowAttributeTypes
from jobmon.attributes.constants import workflow_attribute_types


def load_attribute_types(session):
    """adds list of attributes to a specific attribute_type table"""
    attribute_types = []

    # load attributes for workflow_attribute_types table
    for attribute_type in workflow_attribute_types:
        wf_attribute_types = WorkflowAttributeTypes(name=attribute_type,
                                                    type=getattr(workflow_attribute_types, attribute_type).TYPE)
        attribute_types.append(wf_attribute_types)

    session.add_all(attribute_types)
    session.commit()
    print("populated table")
