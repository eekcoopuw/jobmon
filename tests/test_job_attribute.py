import pytest

from jobmon.workflow.bash_task import BashTask
from jobmon.database import session_scope
from jobmon.attributes.constants import job_attribute


def test_job_attribute(job_list_manager_sub):
    # create a job
    task = BashTask("sleep 1")
    # add an attribute to the task
    task.add_job_attribute(job_attribute.NUM_DRAWS, "10")

    job = job_list_manager_sub.bind_task(task)

    with session_scope() as session:
        # query from job_attribute table
        job_attribute_query = session.execute("""
                                SELECT job_attribute.id,
                                       job_attribute.job_id,
                                       job_attribute.attribute_type,
                                       job_attribute.value
                                FROM job_attribute
                                JOIN job
                                ON job_attribute.job_id=job.job_id
                                WHERE job_attribute.job_id={id}
                                """.format(id=job.job_id))

        attribute_entry = job_attribute_query.fetchone()
        attribute_entry_type = attribute_entry.attribute_type
        attribute_entry_value = attribute_entry.value

        assert attribute_entry_type == job_attribute.NUM_DRAWS
        assert attribute_entry_value == "10"


def test_job_attribute_input_error(job_list_manager_sub):
# create a job
    task = BashTask("sleep 1")
    with pytest.raises(ValueError) as exc:
        task.add_job_attribute("num_locations","fifty")
    assert "Invalid" in str(exc.value)
    job = job_list_manager_sub.bind_task(task)

def test_job_attributes(job_list_manager_sub):
    task = BashTask("sleep 1")
    # add an attribute to the task
    dict_of_attributes = {job_attribute.NUM_DRAWS: "10", job_attribute.NUM_LOCATIONS: "50", job_attribute.NUM_CAUSES: "30"}
    task.add_job_attributes(dict_of_attributes)

    job = job_list_manager_sub.bind_task(task)

    with session_scope() as session:
        # query from job_attribute table
        job_attribute_query = session.execute("""
                                SELECT job_attribute.id,
                                       job_attribute.job_id,
                                       job_attribute.attribute_type,
                                       job_attribute.value
                                FROM job_attribute
                                JOIN job
                                ON job_attribute.job_id=job.job_id
                                WHERE job_attribute.job_id={id}
                                """.format(id=job.job_id))

        attribute_entries = job_attribute_query.fetchall()
        for entry in attribute_entries:
            attribute_entry_type = entry.attribute_type
            attribute_entry_value = entry.value
            assert dict_of_attributes[attribute_entry_type] == attribute_entry_value
