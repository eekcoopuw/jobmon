import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.attributes.constants import job_attribute


def test_job_attribute(job_list_manager_sub):
    print("in test_job_attribute")
    from jobmon.server.database import session_scope

    # create a job
    task = BashTask("sleep 1")
    job = job_list_manager_sub.bind_task(task)

    # add an attribute to the job
    job_list_manager_sub.add_job_attribute(job, job_attribute.NUM_DRAWS, "10")

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
    job = job_list_manager_sub.bind_task(task)

    # add an attribute with wrong types to the workflow
    with pytest.raises(ValueError) as exc:
        job_list_manager_sub.add_job_attribute(job, "num_locations", "fifty")
    assert "Invalid" in str(exc.value)
