import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.requester import Requester
from jobmon.attributes.constants import workflow_attribute, job_attribute
from jobmon.attributes.attribute_models import WorkflowAttribute, JobAttribute


def test_workflow_attribute(real_dag):
    from jobmon.server.database import ScopedSession
    wfa = "workflow_with_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1")
    workflow.add_tasks([t1])
    workflow.execute()

    # add an attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute.NUM_YEARS, "100")

    # query from workflow_attribute table
    workflow_attribute_query = ScopedSession.execute("""
        SELECT wf_att.id, wf_att.workflow_id,
               wf_att.attribute_type, wf_att.value
        FROM workflow_attribute as wf_att
        JOIN workflow
        ON wf_att.workflow_id = workflow.id
        WHERE wf_att.workflow_id = {id}""".format(id=workflow.id))

    workflow_attribute_entry = workflow_attribute_query.fetchone()
    workflow_attribute_entry_type = workflow_attribute_entry.attribute_type
    workflow_attribute_entry_value = workflow_attribute_entry.value
    ScopedSession.commit()

    assert workflow_attribute_entry_type == workflow_attribute.NUM_YEARS
    assert workflow_attribute_entry_value == "100"


def test_workflow_attribute_input_error(real_jsm_jqs, db_cfg):
    wfa = "workflow_with_wrong_arg_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1")
    workflow.add_tasks([t1])
    workflow.execute()

    # add an attribute with wrong types to the workflow
    with pytest.raises(ValueError) as exc:
        workflow.add_workflow_attribute(7.8, "dog")
    assert "Invalid attribute" in str(exc.value)


def test_workflow_attribute_tag(real_jsm_jqs, db_cfg):
    from jobmon.server.database import ScopedSession
    wfa = "workflow_with_tag_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1")
    workflow.add_tasks([t1])
    workflow.execute()

    # add a tag attribute to the workflow
    workflow.add_workflow_attribute(workflow_attribute.TAG, "dog")

    workflow_attribute_query = ScopedSession.query(WorkflowAttribute).first()

    workflow_attribute_entry_type = workflow_attribute_query.attribute_type
    workflow_attribute_entry_value = workflow_attribute_query.value

    assert workflow_attribute_entry_type == workflow_attribute.TAG
    assert workflow_attribute_entry_value == "dog"


def test_attributes_on_workflow_retrievable(simple_workflow):
    # add attributes to workflow and jobs
    simple_workflow.add_workflow_attribute(workflow_attribute.TAG, 'tester')
    for job in simple_workflow.task_dag.job_list_manager.all_done:
        simple_workflow.task_dag.job_list_manager.add_job_attribute(
            job, job_attribute.TAG, 'tester')

    # make sure we can get those attributes back
    from jobmon.client.the_client_config import get_the_client_config
    req = Requester(get_the_client_config(), 'jqs')
    return_code, resp = req.send_request('/workflow/{}/workflow_attribute'
        .format(simple_workflow.id),
        {'workflow_attribute_type': workflow_attribute.TAG}, 'get')
    assert resp['workflow_attr_dct'][0]['value'] == 'tester'

    for job in simple_workflow.task_dag.job_list_manager.all_done:
        return_code, resp = req.send_request('/workflow/{}/job_attribute'
            .format(simple_workflow.id),
            {'job_attribute_type': job_attribute.TAG}, 'get')
        assert resp['job_attr_dct'][0]['value'] == 'tester'
