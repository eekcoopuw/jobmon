import pytest
from time import sleep

from jobmon import sge
from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    jlm = JobListManager(dag_id, executor=execute_sge,
                         start_daemons=True)
    yield jlm
    jlm.disconnect()


def test_valid_command(dag_id, job_list_manager_sge):
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/jmtest.sh"),
        "sge_foobar", max_attempts=3)
    job_list_manager_sge.queue_job(job_id)
    sleep(60)  # Give some time for the job to get to the executor
    done = job_list_manager_sge.get_new_done()
    assert len(done) == 1
