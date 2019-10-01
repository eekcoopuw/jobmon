import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client import shared_requester as req
from jobmon.models.attributes.constants import workflow_attribute
from jobmon.models.attributes.workflow_attribute import WorkflowAttribute


@pytest.mark.qsubs_jobs
def test_workflow_attribute(db_cfg, env_var):
    wfa = "workflow_with_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow.execute()

    # add an attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute.NUM_YEARS, "100")

    # query from workflow_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        workflow_attribute_query = DB.session.execute("""
            SELECT wf_att.id, wf_att.workflow_id,
                   wf_att.attribute_type, wf_att.value
            FROM workflow_attribute as wf_att
            JOIN workflow
            ON wf_att.workflow_id = workflow.id
            WHERE wf_att.workflow_id = {id}""".format(id=workflow.id))

        workflow_attribute_entry = workflow_attribute_query.fetchone()
        workflow_attribute_entry_type = workflow_attribute_entry.attribute_type
        workflow_attribute_entry_value = workflow_attribute_entry.value
        DB.session.commit()

        assert workflow_attribute_entry_type == workflow_attribute.NUM_YEARS
        assert workflow_attribute_entry_value == "100"


@pytest.mark.qsubs_jobs
def test_workflow_attribute_input_error(env_var, db_cfg):
    wfa = "workflow_with_wrong_arg_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow.execute()

    # add an attribute with wrong types to the workflow
    with pytest.raises(ValueError) as exc:
        workflow.add_workflow_attribute(7.8, "dog")
    assert "Invalid attribute" in str(exc.value)


@pytest.mark.qsubs_jobs
def test_workflow_attribute_tag(env_var, db_cfg):
    wfa = "workflow_with_tag_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow.execute()

    # add a tag attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute.TAG, "dog")

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        workflow_attribute_query = DB.session.query(WorkflowAttribute).\
            first()

        workflow_attribute_entry_type = workflow_attribute_query.attribute_type
        workflow_attribute_entry_value = workflow_attribute_query.value

        assert workflow_attribute_entry_type == workflow_attribute.TAG
        assert workflow_attribute_entry_value == "dog"

    # make sure we can get those attributes back
    return_code, resp = req.send_request(
        '/workflow/{}/workflow_attribute'.format(workflow.id),
        {'workflow_attribute_type': workflow_attribute.TAG}, 'get')
    assert resp['workflow_attr_dct'][0]['value'] == 'dog'
