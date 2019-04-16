import pytest
import time
from time import sleep

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.client import shared_requester as req
from jobmon.client.swarm.executors.dummy import DummyExecutor
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.server.jobmonLogging import jobmonLogging as logging

from tests.timeout_and_skip import timeout_and_skip
from functools import partial

logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def job_list_manager_dummy(real_dag_id):
    # We don't want this queueing jobs in conflict with the SGE daemons...
    # but we do need it to subscribe to status updates for reconciliation
    # tests. Start this thread manually.
    executor = DummyExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=False, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_reconciliation(real_dag_id):
    executor = SGEExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=True, reconciliation_interval=2,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


def test_reconciler_dummy(job_list_manager_dummy):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_dummy.all_error = set()

    # Queue a job
    task = Task(command="ls", num_cores="1", name="dummyfbb", max_attempts=1)
    job = job_list_manager_dummy.bind_task(task)
    job_list_manager_dummy.queue_job(job)
    job_list_manager_dummy.job_instance_factory.instantiate_queued_jobs()

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job remains gets moved to error during reconciliation
    # because we only allow 1 attempt
    sleep(10)
    job_list_manager_dummy.job_inst_reconciler.reconcile()
    job_list_manager_dummy._sync()
    assert job_list_manager_dummy.all_error


def test_reconciler_sge(job_list_manager_reconciliation):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Queue a job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_pass")
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    # Give the job_state_manager some time to process the error message
    # This test job just sleeps for 30s, so it should not be missing
    # DO NOT put in a while-True loop
    sleep(10)
    jir = job_list_manager_reconciliation.job_inst_reconciler
    jir.reconcile()

    job_list_manager_reconciliation._sync()
    assert len(job_list_manager_reconciliation.all_error) == 0


def test_reconciler_sge_timeout(job_list_manager_reconciliation):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Queue a test job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_fail", max_attempts=3, max_runtime_seconds=3,
                slots=1)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job.
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds (well, when the reconciler runs, typically every 10 seconds)
    # 60
    timeout_and_skip(20, 100, 1, "sleepyjob_fail", partial(
        reconciler_sge_timeout_check,
        job_list_manager_reconciliation=job_list_manager_reconciliation,
        dag_id=job_list_manager_reconciliation.dag_id,
        job_id=job.job_id))


def reconciler_sge_timeout_check(job_list_manager_reconciliation, dag_id,
                                 job_id):
    job_list_manager_reconciliation._sync()
    if len(job_list_manager_reconciliation.all_error) == 1:
        assert job_id in [
            j.job_id for j in job_list_manager_reconciliation.all_error]

        # The job should have been tried 3 times...
        _, response = req.send_request(
            app_route='/dag/{}/job'.format(dag_id),
            message={},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
        this_job = [j for j in jobs if j.job_id == job_id][0]
        assert this_job.num_attempts == 3
        return True
    else:
        return False


def test_ignore_qw_in_timeouts(job_list_manager_reconciliation):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Qsub a long running job -> queue another job that waits on it,
    # to simulate a hqw -> set the timeout for that hqw job to something
    # short... make sure that job doesn't actually get killed
    # TBD I don't think that has been implemented.
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob", max_attempts=3, max_runtime_seconds=3,
                slots=1)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds

    timeout_and_skip(10, 90, 1, "sleepyjob", partial(
        ignore_qw_in_timeouts_check,
        job_list_manager_reconciliation=job_list_manager_reconciliation,
        dag_id=job_list_manager_reconciliation.dag_id,
        job_id=job.job_id))


def ignore_qw_in_timeouts_check(job_list_manager_reconciliation, dag_id, job_id
                                ):
    job_list_manager_reconciliation._sync()
    if len(job_list_manager_reconciliation.all_error) == 1:
        assert job_id in [
            j.job_id for j in job_list_manager_reconciliation.all_error]

        # The job should have been tried 3 times...
        _, response = req.send_request(
            app_route='/dag/{}/job'.format(dag_id),
            message={},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
        this_job = [j for j in jobs if j.job_id == job_id][0]
        assert this_job.num_attempts == 3
        return True
    else:
        return False


@pytest.fixture(scope='function')
def sge_jlm_for_queues(real_dag_id, tmpdir_factory):
    """This creates a job_list_manager that uses the SGEExecutor, does
    start the JobInstanceFactory and JobReconciler threads, and does not
    interrupt on error. It has short reconciliation intervals so that the
    tests run faster than in production.
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_jenkins')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False,
                         reconciliation_interval=10,
                         job_instantiation_interval=10,
                         interrupt_on_error=True, n_queued_jobs=3)
    yield jlm
    jlm.disconnect()


def test_queued_for_instantiation(sge_jlm_for_queues):

    test_jif = sge_jlm_for_queues.job_instance_factory

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", slots=1)
        tasks.append(task)
        job = sge_jlm_for_queues.bind_task(task)
        sge_jlm_for_queues.queue_job(job)

    # comparing results and times of old query vs new query
    time_a = time.time()
    rc, response = test_jif.requester.send_request(
        app_route=f'/dag/{test_jif.dag_id}/job',
        message={'status': JobStatus.QUEUED_FOR_INSTANTIATION},
        request_type='get')
    all_jobs = [Job.from_wire(j) for j in response['job_dcts']]
    time_b = time.time()

    # now new query that should only return 3 jobs
    select_jobs = test_jif._get_jobs_queued_for_instantiation()
    time_c = time.time()
    first_query = time_b - time_a
    new_query = time_c - time_b

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20
    for i in range(3):
        assert select_jobs[i].job_id == (i+1)
