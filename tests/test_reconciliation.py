import pytest
import sys
from time import sleep

from jobmon import sge
from jobmon.job_instance_factory import execute_batch_dummy, execute_sge
from jobmon.job_list_manager import JobListManager

from tests.timeout_and_skip import timeout_and_skip

if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


@pytest.fixture(scope='function')
def job_list_manager_dummy(dag_id):
    # We don't want this queueing jobs in conflict with the SGE daemons...
    # but we do need it to subscribe to status updates for reconciliation
    # tests. Start this thread manually.
    jlm = JobListManager(dag_id, executor=execute_batch_dummy,
                         start_daemons=False)
    jlm._start_job_status_listener()
    return jlm


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    jlm = JobListManager(dag_id, executor=execute_sge,
                         start_daemons=True, reconciliation_interval=2)
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
    job_list_manager_dummy.get_new_errors()

    # Queue a job
    job_id = job_list_manager_dummy.create_job("ls", "dummyfbb", "hash")
    job_list_manager_dummy.queue_job(job_id)
    job_list_manager_dummy.job_inst_factory.instantiate_queued_jobs()

    jir = job_list_manager_dummy.job_inst_reconciler
    exec_ids = jir._get_presumed_submitted_or_running()
    assert len(exec_ids) == 1

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job remains in 'instantiated' state and does not
    # appear in qstat (unless we get super unlucky with random numbers...) and
    # thus should resolve to missing.
    # TODO: Fix that 'super unlucky' bit
    jir.reconcile()

    timeout_and_skip(5, 30, 1, partial(
        reconciler_dummy_check,
        job_list_manager_dummy=job_list_manager_dummy,
        job_id=job_id))


def reconciler_dummy_check(job_list_manager_dummy, job_id):
    errors = job_list_manager_dummy.get_new_errors()
    if len(errors) == 1:
        assert errors == [job_id]
        return True
    else:
        return False


def test_reconciler_sge(jsm_jqs, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    errors = job_list_manager_sge.get_new_errors()

    # Queue a job
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/sleep.sh"), "sleepyjob", "hash")
    job_list_manager_sge.queue_job(job_id)

    # Give the job_state_manager some time to process the error message
    # This test job just sleeps for 30s, so it should not be missing
    # DO NOT put in a while-True loop
    sleep(10)
    jir = job_list_manager_sge.job_inst_reconciler
    jir.reconcile()

    errors = job_list_manager_sge.get_new_errors()
    assert len(errors) == 0

    # Artificially advance job to DONE so it doesn't impact downstream tests
    jsm, _ = jsm_jqs
    for job_instance in jir._get_presumed_submitted_or_running():
        try:
            # In case the job never actually got out of qw due to a busy
            # cluster
            jsm.log_running(job_instance.job_instance_id,
                            nodename='not_a_node', process_group_id=1234)
        except Exception as e:
            print(e)
        jsm.log_done(job_instance.job_instance_id)


def test_reconciler_sge_timeout(jsm_jqs, dag_id, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_sge.get_new_errors()

    # Queue a test job
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/sleep.sh"), "sleepyjob", "hash",
        max_attempts=3, max_runtime=3)
    job_list_manager_sge.queue_job(job_id)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job.
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds (well, when the reconciler runs, typically every 10 seconds)

    # 60
    timeout_and_skip(10, 90, 1, partial(
        reconciler_sge_timeout_check,
        job_list_manager_sge=job_list_manager_sge,
        jsm_jqs=jsm_jqs,
        dag_id=dag_id,
        job_id=job_id))


def reconciler_sge_timeout_check(job_list_manager_sge, jsm_jqs, dag_id,
                                 job_id):
    errors = job_list_manager_sge.get_new_errors()
    if len(errors) == 1:
        assert job_id in errors

        # The job should have been tried 3 times...
        _, jqs = jsm_jqs
        rc, jobs = jqs.get_jobs(dag_id)
        this_job = [j for j in jobs if j['job_id'] == job_id][0]
        assert this_job['num_attempts'] == 3
        return True
    else:
        return False


def test_ignore_qw_in_timeouts(jsm_jqs, dag_id, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_sge.get_new_errors()

    # Qsub a long running job -> queue another job that waits on it,
    # to simulate a hqw -> set the timeout for that hqw job to something
    # short... make sure that job doesn't actually get killed
    # TBD I don't think that has been implemented.
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/sleep.sh"), "sleepyjob", "hash",
        max_attempts=3, max_runtime=3)
    job_list_manager_sge.queue_job(job_id)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds

    timeout_and_skip(10, 90, 1, partial(
        ignore_qw_in_timeouts_check,
        job_list_manager_sge=job_list_manager_sge,
        jsm_jqs=jsm_jqs,
        dag_id=dag_id,
        job_id=job_id))


def ignore_qw_in_timeouts_check(job_list_manager_sge, jsm_jqs, dag_id, job_id):
    errors = job_list_manager_sge.get_new_errors()
    if len(errors) == 1:
        assert job_id in errors

        # The job should have been tried 3 times...
        _, jqs = jsm_jqs
        rc, jobs = jqs.get_jobs(dag_id)
        this_job = [j for j in jobs if j['job_id'] == job_id][0]
        assert this_job['num_attempts'] == 3
        return True
    else:
        return False
