import pytest
import sys
from time import sleep

from jobmon import sge
from jobmon.models import Job
from jobmon.requester import Requester
from jobmon.executors.dummy import DummyExecutor
from jobmon.executors.sge import SGEExecutor
from jobmon.job_list_manager import JobListManager
from jobmon.workflow.executable_task import ExecutableTask as Task

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
    executor = DummyExecutor()
    jlm = JobListManager(dag_id, executor=executor,
                         start_daemons=False, interrupt_on_error=False)
    return jlm


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    executor = SGEExecutor()
    jlm = JobListManager(dag_id, executor=executor,
                         start_daemons=True, reconciliation_interval=2,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_dummy_nod(dag_id):
    executor = DummyExecutor()
    jlm = JobListManager(dag_id, executor=dummy_executor,
                         start_daemons=False, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


def test_reconciler_dummy(job_list_manager_dummy):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_dummy.all_error = set()

    # Queue a job
    task = Task(command="ls", name="dummyfbb")
    job = job_list_manager_dummy.bind_task(task)
    job_list_manager_dummy.queue_job(job)
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

    sleep(10)
    reconciler_dummy_check(
        job_list_manager_dummy=job_list_manager_dummy,
        job_id=job.job_id)


def reconciler_dummy_check(job_list_manager_dummy, job_id):
    job_list_manager_dummy.all_error = set()
    if len(job_list_manager_dummy.all_error) == 1:
        assert job_list_manager_dummy.all_error == [job_id]
        return True
    else:
        return False


def test_reconciler_sge(jsm_jqs, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_sge.all_error = set()

    # Queue a job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_pass")
    job = job_list_manager_sge.bind_task(task)
    job_list_manager_sge.queue_job(job)

    # Give the job_state_manager some time to process the error message
    # This test job just sleeps for 30s, so it should not be missing
    # DO NOT put in a while-True loop
    sleep(10)
    jir = job_list_manager_sge.job_inst_reconciler
    jir.reconcile()

    job_list_manager_sge.get_job_statuses()
    assert len(job_list_manager_sge.all_error) == 0

    # Artificially advance job to DONE so it doesn't impact downstream tests
    jsm, _ = jsm_jqs
    for job_instance in jir._get_presumed_submitted_or_running():
        req = jir.jsm_req
        try:
            # In case the job never actually got out of qw due to a busy
            # cluster
            req.send_request(
                app_route='/log_running',
                message={'job_instance_id': job_instance.job_instance_id,
                         'nodename': "not_a_node",
                         'process_group_id': str(1234)},
                request_type='post')
        except Exception as e:
            print(e)
        req.send_request(
            app_route='log_done',
            message={'job_instance_id': job_instance.job_instance_id},
            request_type='post')


def test_reconciler_sge_timeout(jsm_jqs, dag_id, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_sge.all_error = set()

    # Queue a test job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_fail", max_attempts=3, max_runtime=3)
    job = job_list_manager_sge.bind_task(task)
    job_list_manager_sge.queue_job(job)

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
        job_id=job.job_id))


def reconciler_sge_timeout_check(job_list_manager_sge, jsm_jqs, dag_id,
                                 job_id):
    from jobmon.config import config
    job_list_manager_sge.get_job_statuses()
    if len(job_list_manager_sge.all_error) == 1:
        assert job_id in [j.job_id for j in job_list_manager_sge.all_error]

        req = Requester(config.jqs_port)

        # The job should have been tried 3 times...
        _, jqs = jsm_jqs
        _, response = req.send_request(
            app_route='/get_jobs',
            message={'dag_id': str(dag_id)},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
        this_job = [j for j in jobs if j.job_id == job_id][0]
        assert this_job.num_attempts == 3
        return True
    else:
        return False


def test_ignore_qw_in_timeouts(jsm_jqs, dag_id, job_list_manager_sge):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_sge.all_error = set()

    # Qsub a long running job -> queue another job that waits on it,
    # to simulate a hqw -> set the timeout for that hqw job to something
    # short... make sure that job doesn't actually get killed
    # TBD I don't think that has been implemented.
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob", max_attempts=3, max_runtime=3)
    job = job_list_manager_sge.bind_task(task)
    job_list_manager_sge.queue_job(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds

    timeout_and_skip(10, 90, 1, partial(
        ignore_qw_in_timeouts_check,
        job_list_manager_sge=job_list_manager_sge,
        jsm_jqs=jsm_jqs,
        dag_id=dag_id,
        job_id=job.job_id))


def ignore_qw_in_timeouts_check(job_list_manager_sge, jsm_jqs, dag_id, job_id):
    from jobmon.config import config
    job_list_manager_sge.get_job_statuses()
    if len(job_list_manager_sge.all_error) == 1:
        assert job_id in [j.job_id for j in job_list_manager_sge.all_error]

        req = Requester(config.jqs_port)

        # The job should have been tried 3 times...
        _, jqs = jsm_jqs
        _, response = req.send_request(
            app_route='/get_jobs',
            message={'dag_id': str(dag_id)},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
        this_job = [j for j in jobs if j.job_id == job_id][0]
        assert this_job.num_attempts == 3
        return True
    else:
        return False
