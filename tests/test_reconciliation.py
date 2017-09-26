import pytest
from time import sleep

from jobmon.job_instance_factory import execute_batch_dummy, execute_sge
from jobmon.job_list_manager import JobListManager


@pytest.fixture(scope='function')
def job_list_manager_dummy(dag_id):
    jlm = JobListManager(dag_id, executor=execute_batch_dummy,
                         start_daemons=True)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    jlm = JobListManager(dag_id, executor=execute_sge,
                         start_daemons=True)
    yield jlm
    jlm.disconnect()


def test_reconciler_dummy(job_list_manager_dummy):
    job_id = job_list_manager_dummy.create_job("ls", "dummyfbb")
    job_list_manager_dummy.queue_job(job_id)
    job_list_manager_dummy.job_inst_factory.instantiate_queued_jobs()

    jir = job_list_manager_dummy.job_inst_reconciler
    exec_ids = jir._get_presumed_instantiated_or_running()
    assert len(exec_ids) == 1

    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_dummy.get_new_errors()

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job remains in 'instantiated' state and does not
    # appear in qstat (unless we get super unlucky with random numbers...) and
    # thus should resolve to missing.
    # TODO: Fix that 'super unlucky' bit
    missing = jir.reconcile()

    # Give the job_state_manager some time to process the error message
    sleep(5)
    errors = job_list_manager_dummy.get_new_errors()
    assert len(errors) == 1
    assert errors == [job_id]


def test_reconciler_sge(job_list_manager_sge):
    job_id = job_list_manager_sge.create_job("~/tmp/jobmon/tests/sleep.sh",
                                             "sleepyjob")
    job_list_manager_sge.queue_job(job_id)
    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_sge.get_new_errors()

    # This test job just sleeps for 10s.. so it should not be missing
    jir = job_list_manager_sge.job_inst_reconciler
    missing = jir.reconcile()

    # Give the job_state_manager some time to process the error message
    sleep(5)
    errors = job_list_manager_sge.get_new_errors()
    assert len(errors) == 0
