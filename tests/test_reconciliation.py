import pytest
from time import sleep

from jobmon import sge
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


@pytest.fixture(scope='function')
def job_list_manager_dummy_nod(dag_id):
    jlm = JobListManager(dag_id, executor=execute_batch_dummy,
                         start_daemons=False)
    yield jlm
    jlm.disconnect()


def test_reconciler_dummy(job_list_manager_dummy):
    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_dummy.get_new_errors()

    # Queue a job
    job_id = job_list_manager_dummy.create_job("ls", "dummyfbb")
    job_list_manager_dummy.queue_job(job_id)
    job_list_manager_dummy.job_inst_factory.instantiate_queued_jobs()

    jir = job_list_manager_dummy.job_inst_reconciler
    exec_ids = jir._get_presumed_instantiated_or_running()
    assert len(exec_ids) == 1

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job remains in 'instantiated' state and does not
    # appear in qstat (unless we get super unlucky with random numbers...) and
    # thus should resolve to missing.
    # TODO: Fix that 'super unlucky' bit
    jir.reconcile()

    # Give the job_state_manager some time to process the error message
    sleep(5)
    errors = job_list_manager_dummy.get_new_errors()
    assert len(errors) == 1
    assert errors == [job_id]


def test_reconciler_sge(job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_sge.get_new_errors()

    # Queue a job
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/sleep.sh"), "sleepyjob")
    job_list_manager_sge.queue_job(job_id)

    # This test job just sleeps for 10s.. so it should not be missing
    jir = job_list_manager_sge.job_inst_reconciler
    jir.reconcile()

    # Give the job_state_manager some time to process the error message
    sleep(5)
    errors = job_list_manager_sge.get_new_errors()
    assert len(errors) == 0


def test_reconciler_timeout(job_list_manager_dummy_nod):
    job_id = job_list_manager_dummy_nod.create_job("ls", "dummyfbb")
    job_list_manager_dummy_nod.queue_job(job_id)
    job_list_manager_dummy_nod.job_inst_factory.instantiate_queued_jobs()

    jir = job_list_manager_dummy_nod.job_inst_reconciler
    job_instances = jir._get_presumed_instantiated_or_running()
    assert len(job_instances) == 1
    assert job_instances[0].job_id == job_id


def test_reconciler_sge_timeout(job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_sge.get_new_errors()

    # Queue a test job
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/sleep.sh"), "sleepyjob", max_runtime=3)
    job_list_manager_sge.queue_job(job_id)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job
    sleep(5)

    # There should now be a job that has errored out
    errors = job_list_manager_sge.get_new_errors()
    assert len(errors) == 1
    assert job_id in errors
