from functools import partial

import pytest

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.models.attributes.constants import job_attribute, \
    workflow_attribute
from tests.timeout_and_skip import timeout_and_skip


def test_job_attribute(db_cfg, job_list_manager_sge):

    # create a job
    task = BashTask("sleep 1", num_cores=1)

    # add an attribute to the task
    task.add_job_attribute(job_attribute.NUM_DRAWS, "10")

    job = job_list_manager_sge.bind_task(task)

    # add an attribute to the job
    job_list_manager_sge.add_job_attribute(job.job_id, job_attribute.NUM_DRAWS, "10")

    # query from job_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job_attribute_query = DB.session.execute("""
                                SELECT job_attribute.id,
                                       job_attribute.job_id,
                                       job_attribute.attribute_type,
                                       job_attribute.value
                                FROM job_attribute
                                JOIN job
                                ON job_attribute.job_id=job.job_id
                                WHERE job_attribute.job_id={id}
                                """.format(id=job.job_id))
        DB.session.commit()
        attribute_entry = job_attribute_query.fetchone()
        attribute_entry_type = attribute_entry.attribute_type
        attribute_entry_value = attribute_entry.value

        assert attribute_entry_type == job_attribute.NUM_DRAWS
        assert attribute_entry_value == "10"


def test_job_attribute_input_error(job_list_manager_sge):
    # create a job
    task = BashTask("sleep 1", num_cores=1)
    with pytest.raises(ValueError) as exc:
        task.add_job_attribute("num_locations", "fifty")
    assert "Invalid" in str(exc.value)


def test_job_attributes(db_cfg, job_list_manager_sge):

    task = BashTask("sleep 1", num_cores=1)
    # add an attribute to the task
    dict_of_attributes = {job_attribute.NUM_DRAWS: "10",
                          job_attribute.NUM_LOCATIONS: "50",
                          job_attribute.NUM_CAUSES: "30"}
    task.add_job_attributes(dict_of_attributes)

    job = job_list_manager_sge.bind_task(task)

    # query from job_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job_attribute_query = DB.session.execute("""
                                SELECT job_attribute.id,
                                       job_attribute.job_id,
                                       job_attribute.attribute_type,
                                       job_attribute.value
                                FROM job_attribute
                                JOIN job
                                ON job_attribute.job_id=job.job_id
                                WHERE job_attribute.job_id={id}
                                """.format(id=job.job_id))
        DB.session.commit()

        attribute_entries = job_attribute_query.fetchall()
        for entry in attribute_entries:
            attribute_entry_type = entry.attribute_type
            attribute_entry_value = entry.value
            assert (dict_of_attributes[attribute_entry_type] ==
                    attribute_entry_value)


def test_usage_job_attribute_error(job_list_manager_sge):
    task = BashTask("sleep 1", slots=1)
    # Try to add a usage attribute, this should cause an error because this is
    # configured with usage stats, not with user assigned values
    with pytest.raises(ValueError) as exc:
        task.add_job_attribute(job_attribute.WALLCLOCK, "10")
    assert "Invalid attribute configuration" in str(exc.value)

def check_workflow_attribute(workflow: Workflow, workflow_id: int):
    return_code, resp = workflow.requester.send_request(
        '/workflow/{}/workflow_attribute'.format(workflow_id),
        {'job_attribute_type': job_attribute.NUM_DRAWS}, 'get')
    return return_code == 200 and resp['job_attr_dct'][0]['value'] == '10'


def check_job_attribute(workflow: Workflow, job_id: int):
    return_code, resp = workflow.requester.send_request(
        '/job/{}/job_attribute'.format(job_id),
        {'job_attribute_type': job_attribute.NUM_DRAWS}, 'get')
    return return_code == 200 and resp['job_attr_dct'][0]['value'] == '10'


def test_attributes_retrievable(real_jsm_jqs, db_cfg):
    # add attributes to jobs and workflows

    wf = Workflow('test_attributes', project="proj_tools")
    task = BashTask(command=sge.true_path("sleep 5"),
                    num_cores=1)
    task.add_job_attribute(job_attribute.NUM_DRAWS, "10")
    wf.add_task(task)
    wf.run()

    stub_job = wf.task_dag.job_list_manager.hash_job_map[task.hash]
    job_id = stub_job.job_id

    timeout_and_skip(step_size=10, max_time=30, max_qw=1,
                     job_name='check_job_attribute',
                     partial_test_function=partial(
                         check_job_attribute,
                         workflow=wf,
                         job_id=job_id))
