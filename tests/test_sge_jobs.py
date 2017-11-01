import pytest
from time import sleep
from datetime import datetime, timedelta

from jobmon import sge
from jobmon.database import session_scope
from jobmon.models import JobInstance, JobInstanceStatus
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
        "sge_foobar", slots=2, mem_free=4, project="proj_qlogins",
        max_attempts=3)
    job_list_manager_sge.queue_job(job_id)

    total_sleep = 0
    step_size = 10
    while True:
        sleep(step_size)  # Give some time for the job to get to the executor
        total_sleep += step_size
        done = job_list_manager_sge.get_new_done()
        if len(done) == 1:
            # Success
            assert True
            return
        elif total_sleep > 180:
            print(sge.qstat())
            fail_msg = ("Test failed due to 180s timeout: check the previous qtsat output; "
                        "check that you have permission to run under "
                        "'proj_qlogins' and that there are available jobs under this"
                        " project")
            assert len(done) == 1, fail_msg


def test_context_args(jsm_jqs, job_list_manager_sge):
    delay_to = (datetime.now() + timedelta(minutes=5)).strftime("%m%d%H%M")
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/jmtest.sh"),
        "sge_foobar", slots=2, mem_free=4, max_attempts=3,
        context_args={'sge_add_args': '-a {}'.format(delay_to)})
    job_list_manager_sge.queue_job(job_id)

    total_sleep = 0
    step_size = 10
    while True:
        sleep(step_size)  # Give some time for the job to get to the executor
        total_sleep += step_size
        with session_scope() as session:
            jis = session.query(JobInstance).filter_by(job_id=job_id).all()
            njis = len(jis)
            status = jis[0].status
            sge_jid = jis[0].executor_id
        # Make sure the job actually got to SGE
        if njis == 1:
            # Make sure it hasn't advanced to running (i.e. the -a argument worked)
            assert status == JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR
            # Cleanup
            sge.qdel(sge_jid)
            return
        elif total_sleep > 180:
            # done waiting
            print(sge.qstat())
            fail_msg = ("Test failed due to 180s timeout: check the  qtsat output; "
                        "check that you have permission to run under "
                        "'proj_qlogins' and that there are available jobs under this"
                        " project")
            assert njis == 1, fail_msg


