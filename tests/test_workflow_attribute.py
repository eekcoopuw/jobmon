from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.workflow import Workflow
from jobmon.database import session_scope
from jobmon.attributes.constants import workflow_attribute_type


def test_workflow_attribute(dag):
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "workflow_with_attribute"
    workflow = Workflow(dag, wfa)
    workflow.execute()

    # add an attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute_type.NUM_YEARS.ID, "100")

    with session_scope() as session:
        # query from workflow_attributes table
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

        assert workflow_attribute_entry_type == workflow_attribute_type.NUM_YEARS.ID
        assert workflow_attribute_entry_value == "100"
