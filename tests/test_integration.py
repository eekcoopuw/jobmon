import pytest
import sys
from time import sleep

from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask

from tests.timeout_and_skip import timeout_and_skip

from functools import partial


class Task(ExecutableTask):
    """Test version of the Task class for use in this module"""

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


@pytest.fixture(scope='function')
def job_list_manager(real_dag_id):
    jlm = JobListManager(real_dag_id, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_d(real_dag_id):
    jlm = JobListManager(real_dag_id, start_daemons=True,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge_no_daemons(real_dag_id):
    """This fixture starts a JobListManager using the SGEExecutor, but without
    running JobInstanceFactory or JobReconciler in daemonized threads
    """
    executor = SGEExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


def test_sync(job_list_manager_sge_no_daemons):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    now = job_list_manager_sge.last_sync
    assert now is not None

    job = job_list_manager_sge.bind_task(Task(command='fizzbuzz',  name='bar',
                                              slots=1))
    job_list_manager_sge.queue_job(job)
    job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()
    sleep(35)

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs
    job_list_manager_sge._sync()
    new_now = job_list_manager_sge.last_sync
    assert new_now > now
    assert len(job_list_manager_sge.all_error) > 0


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar', slots=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.queue_job(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0

    job_list_manager.job_instance_factory.instantiate_queued_jobs()
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_error) > 0


def test_valid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='ls', name='baz', slots=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0
    assert len(job_list_manager.all_done) == 0

    job_list_manager.queue_job(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_instance_factory.instantiate_queued_jobs()
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="some new job",
                                            name="foobar", slots=1))
    job_list_manager_d.queue_job(job)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, partial(
        daemon_invalid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_invalid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_error) == 1


def test_daemon_valid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="ls", name="foobarbaz",
                                            slots=1))
    job_list_manager_d.queue_job(job)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, partial(
        daemon_valid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_valid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_done) == 1


def test_blocking_update_timeout(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="sleep 3",
                                            name="foobarbaz", slots=1))
    job_list_manager_d.queue_job(job)
    assert job_list_manager_d.block_until_any_done_or_error(timeout=2) is None


def test_sge_valid_command(job_list_manager_sge_no_daemons):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    job = job_list_manager_sge.bind_task(Task(command="ls",
                                              name="sgefbb",
                                              slots=3,
                                              mem_free='6M'))
    job_list_manager_sge.queue_job(job)
    job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()
    job_list_manager_sge._sync()
    assert (job_list_manager_sge.bound_tasks[job.job_id].status ==
            JobStatus.INSTANTIATED)
    print("finishing test_sge_valid_command")
