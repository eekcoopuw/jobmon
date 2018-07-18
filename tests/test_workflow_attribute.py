from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.workflow import Workflow
from jobmon.attributes.attribute_models import WorkflowAttributes
from jobmon.database import session_scope
from jobmon.attributes.constants import workflow_attribute_types


def test_workflow_attribute(dag):
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "workflow_with_attribute"
    workflow = Workflow(dag, wfa)

    # add an attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute_types.NUM_YEARS.ID, "100")

    with session_scope() as session:
        # query from workflow_attributes table
        workflow_attribute = session.query(WorkflowAttributes).first()
        workflow_attribute_type = workflow_attribute.attribute_type
        workflow_attribute_value = workflow_attribute.value

        assert workflow_attribute_type == workflow_attribute_types.NUM_YEARS.ID
        assert workflow_attribute_value == "100"
