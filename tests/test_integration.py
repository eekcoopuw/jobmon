from builtins import str
import pytest
import sys
from queue import Empty

from jobmon import models
from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager
from jobmon.workflow.executable_task import ExecutableTask

from tests.timeout_and_skip import timeout_and_skip

if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


class Task(ExecutableTask):

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


@pytest.fixture(scope='function')
def job_list_manager(real_dag_id):
    jlm = JobListManager(real_dag_id, interrupt_on_error=False)
    yield jlm


@pytest.fixture(scope='function')
def job_list_manager_d(real_dag_id):
    jlm = JobListManager(real_dag_id, start_daemons=True,
                         interrupt_on_error=False)
    yield jlm


@pytest.fixture(scope='function')
def job_list_manager_sge(real_dag_id):
    jlm = JobListManager(real_dag_id, executor=execute_sge,
                         interrupt_on_error=False)
    yield jlm


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar'))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.queue_job(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0

    job_list_manager.job_inst_factory.instantiate_queued_jobs()
    job_list_manager.get_job_statuses()
    assert len(job_list_manager.all_error) > 0


def test_valid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='ls', name='baz'))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0
    assert len(job_list_manager.all_done) == 0

    job_list_manager.queue_job(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_inst_factory.instantiate_queued_jobs()
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="some new job",
                                            name="foobar"))
    job_list_manager_d.queue_job(job)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, partial(
        daemon_invalid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_invalid_command_check(job_list_manager_d):
    errors = job_list_manager_d.get_new_errors()
    return len(errors) == 1


def test_daemon_valid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="ls", name="foobarbaz"))
    job_list_manager_d.queue_job(job)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, partial(
        daemon_valid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_valid_command_check(job_list_manager_d):
    done = job_list_manager_d.get_new_done()
    return len(done) == 1


def test_blocking_updates(job_list_manager_d):

    # Test 1 job
    job = job_list_manager_d.bind_task(Task(command="sleep 1",
                                            name="foobarbaz"))
    job_list_manager_d.queue_job(job)
    done, _ = job_list_manager_d.block_until_any_done_or_error()
    assert len(done) == 1
    assert done[0].job_id == job.job_id
    assert done[0].status == models.JobStatus.DONE

    # Test multiple jobs

    job_list_manager_d.get_new_done()  # clear the done queue for this test
    job_list_manager_d.get_new_errors()  # clear the error queue too
    job1 = job_list_manager_d.bind_task(Task(command="sleep 2",
                                             name="foobarbaz1"))
    job2 = job_list_manager_d.bind_task(Task(command="sleep 3",
                                             name="foobarbaz2"))
    job3 = job_list_manager_d.bind_task(Task(command="not a command",
                                             name="foobarbaz2"))
    job_list_manager_d.queue_job(job1)
    job_list_manager_d.queue_job(job2)
    job_list_manager_d.queue_job(job3)

    timeout_and_skip(3, 30, 1, partial(
        blocking_updates_check,
        job_list_manager_d=job_list_manager_d,
        job_id1=job1.job_id,
        job_id2=job2.job_id,
        job_id3=job3.job_id)
    )


def blocking_updates_check(job_list_manager_d, job_id1, job_id2, job_id3):
    done, errors = job_list_manager_d.block_until_no_instances(
        raise_on_any_error=False)
    if len(done) == 2:
        assert len(errors) == 1
        assert set(done) == set([job_id1, job_id2])
        assert set(errors) == set([job_id3])
        return True
    else:
        return False


def test_blocking_update_timeout(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="sleep 3",
                                            name="foobarbaz"))
    job_list_manager_d.queue_job(job)
    with pytest.raises(Empty):
        job_list_manager_d.block_until_any_done_or_error(timeout=2)


def test_sge_valid_command(job_list_manager_sge):
    job = job_list_manager_sge.bind_task(Task(command="ls",
                                              name="sgefbb",
                                              slots=3,
                                              mem_free=6))
    job_list_manager_sge.queue_job(job)
    job_list_manager_sge.job_inst_factory.instantiate_queued_jobs()
    assert (job_list_manager_sge.bound_tasks[job.job_id].status ==
            models.JobStatus.INSTANTIATED)
