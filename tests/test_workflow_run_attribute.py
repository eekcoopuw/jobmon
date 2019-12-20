import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.models.attributes.constants import workflow_run_attribute
from tests.conftest import teardown_db


def test_workflow_run_attribute(db_cfg, env_var):
    teardown_db(db_cfg)
    # create a workflow_run
    wfa = "test_workflow_run_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute to workflow_run
    workflow_run.add_workflow_run_attribute(workflow_run_attribute.NUM_DRAWS,
                                            "1000")

    # cleanup
    workflow.task_dag.disconnect()

    # query from workflow_run_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        attribute_query = DB.session.execute("""
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
            AND wf_run_att.attribute_type = {ty}
            """.format(id=workflow_run.id, ty=workflow_run_attribute.NUM_DRAWS))

        attribute_entry = attribute_query.fetchone()
        entry_type = attribute_entry.attribute_type
        entry_value = attribute_entry.value
        DB.session.commit()

        assert entry_type == workflow_run_attribute.NUM_DRAWS
        assert entry_value == "1000"
    teardown_db(db_cfg)


def test_workflow_run_attribute_input_error(db_cfg, env_var):
    teardown_db(db_cfg)
    # create a workflow_run
    wfa = "test_workflow_run_attribute_input_error"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute with wrong types to the workflow_run
    with pytest.raises(ValueError) as exc:
        workflow_run.add_workflow_run_attribute("num_draws", "ten")
    assert "Invalid" in str(exc.value)

    workflow.task_dag.disconnect()
    teardown_db(db_cfg)

