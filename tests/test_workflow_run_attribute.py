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
    workflow_run.add_workflow_run_attribute(workflow_run_attribute.NUM_DRAWS,
                                            "1000")

    with session_scope() as session:
        # query from workflow_run_attribute table
        attribute_query = session.execute("""
                                SELECT wf_run_att.id,
                                       wf_run_att.workflow_run_id,
                                       wf_run_att.attribute_type,
                                       wf_run_att.value
                                FROM workflow_run_attribute
                                     as wf_run_att
                                JOIN workflow_run as wf_run
                                ON wf_run_att.workflow_run_id
                                   =wf_run.id 
                                WHERE wf_run_att.workflow_run_id 
                                      ={id}
                                """.format(id=workflow_run.id))

        attribute_entry = attribute_query.fetchone()
        entry_type = attribute_entry.attribute_type
        entry_value = attribute_entry.value

        assert entry_type == workflow_run_attribute.NUM_DRAWS
        assert entry_value == "1000"


def test_workflow_run_attribute_input_error(dag):
    # create a workflow_run
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "test_workflow_run_attribute_input_error"
    workflow = Workflow(dag, wfa)
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute with wrong types to the workflow_run
    with pytest.raises(ValueError) as exc:
        workflow_run.add_workflow_run_attribute("num_draws", "ten")
    assert "Invalid" in str(exc.value)
