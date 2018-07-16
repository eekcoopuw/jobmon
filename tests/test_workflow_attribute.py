import pytest
from time import sleep
import os

from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.workflow import Workflow
from jobmon.attributes.attribute_models import WorkflowAttributes
from jobmon.database import session_scope


def test_workflow_attribute(dag):
    t1 = BashTask("sleep 1")
    dag.add_tasks([t1])

    wfa = "workflow_with_attribute"
    workflow = Workflow(dag, wfa)
    workflow.execute()
    workflow.add_workflow_attribute(1, "100")

    # import pdb
    # pdb.set_trace()

    with session_scope() as session:
        workflow_attribute = session.query(WorkflowAttributes).first()
        workflow_attribute_type = workflow_attribute.attribute_type
        workflow_attribute_value = workflow_attribute.value

        assert workflow_attribute_type == 1
        assert workflow_attribute_value == "100"