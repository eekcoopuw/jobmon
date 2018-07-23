import pytest

from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.workflow import Workflow
from jobmon.database import session_scope
from jobmon.attributes.constants import workflow_run_attribute


def test_workflow_run_attribute(dag):
    # create a workflow_run
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "test_workflow_run_attribute"
    workflow = Workflow(dag, wfa)
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute to workflow_run
    workflow_run.add_workflow_run_attribute(workflow_run_attribute.NUM_DRAWS, "1000")

    with session_scope() as session:
        # query from workflow_run_attribute table
        workflow_run_attribute_query = session.execute("""
                                            SELECT workflow_run_attribute.id,
                                                   workflow_run_attribute.workflow_run_id,
                                                   workflow_run_attribute.attribute_type,
                                                   workflow_run_attribute.value
                                            FROM workflow_run_attribute 
                                            JOIN workflow_run 
                                            ON workflow_run_attribute.workflow_run_id = workflow_run.id 
                                            WHERE workflow_run_attribute.workflow_run_id 
                                                  = {id}""".format(id=workflow_run.id))

        workflow_run_attribute_entry = workflow_run_attribute_query.fetchone()
        workflow_run_attribute_entry_type = workflow_run_attribute_entry.attribute_type
        workflow_run_attribute_entry_value = workflow_run_attribute_entry.value

        assert workflow_run_attribute_entry_type == workflow_run_attribute.NUM_DRAWS
        assert workflow_run_attribute_entry_value == "1000"

