import pytest

from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.workflow import Workflow
from jobmon.database import session_scope
from jobmon.attributes.constants import workflow_attribute


def test_workflow_attribute(dag):
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "workflow_with_attribute"
    workflow = Workflow(dag, wfa)
    workflow.execute()

    # add an attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute.NUM_YEARS, "100")

    with session_scope() as session:
        # query from workflow_attribute table
        workflow_attribute_query = session.execute("""
                                        SELECT wf_att.id, wf_att.workflow_id,
                                               wf_att.attribute_type, wf_att.value
                                        FROM workflow_attribute as wf_att
                                        JOIN workflow
                                        ON wf_att.workflow_id = workflow.id 
                                        WHERE wf_att.workflow_id = {id}""".format(id=workflow.id))

        workflow_attribute_entry = workflow_attribute_query.fetchone()
        workflow_attribute_entry_type = workflow_attribute_entry.attribute_type
        workflow_attribute_entry_value = workflow_attribute_entry.value

        assert workflow_attribute_entry_type == workflow_attribute.NUM_YEARS
        assert workflow_attribute_entry_value == "100"


def test_workflow_attribute_input_error(dag):
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "workflow_with_attribute"
    workflow = Workflow(dag, wfa)
    workflow.execute()

    # add an attribute with wrong types to the workflow
    with pytest.raises(ValueError) as excinfo:
        workflow.add_workflow_attribute("num_location", 5)
    assert str(excinfo.value) == "Invalid attribute input."
