import pytest

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.models.attributes.constants import job_attribute
from tests.conftest import teardown_db


def test_job_attribute(db_cfg, jlm_sge_no_daemon):
    teardown_db(db_cfg)
    # create a job
    task = BashTask("sleep 1", num_cores=1)

    # add an attribute to the task
    task.add_job_attribute(job_attribute.NUM_DRAWS, "10")

    job = jlm_sge_no_daemon.bind_task(task)

    # add an attribute to the job
    jlm_sge_no_daemon.add_job_attribute(job.job_id, job_attribute.NUM_DRAWS,
                                        "10")

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
    teardown_db(db_cfg)


def test_job_attribute_input_error(env_var):
    # create a job
    task = BashTask("sleep 1", num_cores=1)
    with pytest.raises(ValueError) as exc:
        task.add_job_attribute("num_locations", "fifty")
    assert "Invalid" in str(exc.value)


def test_job_attributes(db_cfg, jlm_sge_no_daemon):
    teardown_db(db_cfg)
    task = BashTask("sleep 1", num_cores=1)
    # add an attribute to the task
    dict_of_attributes = {job_attribute.NUM_DRAWS: "10",
                          job_attribute.NUM_LOCATIONS: "50",
                          job_attribute.NUM_CAUSES: "30"}
    task.add_job_attributes(dict_of_attributes)

    job = jlm_sge_no_daemon.bind_task(task)

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
    teardown_db(db_cfg)


def test_usage_job_attribute_error(db_cfg, env_var):
    teardown_db(db_cfg)
    task = BashTask("sleep 1", num_cores=1)
    # Try to add a usage attribute, this should cause an error because this is
    # configured with usage stats, not with user assigned values
    with pytest.raises(ValueError) as exc:
        task.add_job_attribute(job_attribute.WALLCLOCK, "10")
    assert "Invalid attribute configuration" in str(exc.value)
    teardown_db(db_cfg)


def test_attributes_retrievable(db_cfg, env_var):
    # add attributes to jobs
    teardown_db(db_cfg)
    wf = Workflow('test_attributes', project="proj_tools")
    task = BashTask(command=sge.true_path("sleep 5"),
                    num_cores=1)
    task2 = BashTask(command=sge.true_path("sleep 3"), num_cores=1)

    task.add_job_attribute(job_attribute.NUM_DRAWS, "10")
    task.add_job_attribute(job_attribute.NUM_YEARS, "4")
    task2.add_job_attribute(job_attribute.NUM_YEARS, "3")
    wf.add_tasks([task, task2])
    wf.run()

    stub_job = wf.task_dag.job_list_manager.hash_job_map[task.hash]
    job_id = stub_job.job_id


    return_code, resp = wf.requester.send_request(
        '/job/{}/job_attribute'.format(job_id),
        {'job_attribute_type': job_attribute.NUM_DRAWS}, 'get')
    assert return_code == 200 and resp['job_attr_dct'][0]['value'] == '10'

    return_code, resp = wf.requester.send_request(
        f'/workflow/{wf.id}/job_attribute',
        {'job_attribute_type': job_attribute.NUM_YEARS}, 'get')
    assert return_code == 200
    for el in resp['job_attr_dct']:
        if el['job_id'] == 1:
            assert el['value'] == '4'
        elif el['job_id'] == 2:
            assert el['value'] == '3'
    teardown_db(db_cfg)
