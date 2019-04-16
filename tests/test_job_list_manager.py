import pytest
from time import sleep
from unittest import mock

from tenacity import stop_after_attempt
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.client import shared_requester
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
    """Quick job_instantiation_interval for quick tests"""
    jlm = JobListManager(real_dag_id, start_daemons=True,
                         reconciliation_interval=10,
                         job_instantiation_interval=1,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.fixture(scope='function')
def job_list_manager_sge_no_daemons(real_dag_id):
    """This fixture starts a JobListManager using the SGEExecutor, but without
    running JobInstanceFactory or JobReconciler in daemonized threads.
    Quick job_instantiation_interval for quick tests.
    """
    executor = SGEExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         reconciliation_interval=10,
                         job_instantiation_interval=1,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


def get_presumed_submitted_or_running(dag_id):
    try:
        rc, response = shared_requester.send_request(
            app_route=f'/dag/{dag_id}/job_instance_executor_ids',
            message={'status': [
                JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                JobInstanceStatus.RUNNING]},
            request_type='get')
        job_instances = response['jiid_exid_tuples']
    except TypeError:
        job_instances = []
    return job_instances


def test_sync(job_list_manager_sge_no_daemons):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    now = job_list_manager_sge.last_sync
    assert now is not None

    # This job will intentionally fail
    job = job_list_manager_sge.bind_task(Task(command='fizzbuzz', name='bar',
                                              mem_free='1G',
                                              num_cores=1))
    # create job instances
    job_list_manager_sge.queue_job(job)
    jid = job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()

    # check that the job clears the queue by checking the jqs for any
    # submitted or running
    max_sleep = 600  # 10 min max till test fails
    slept = 0
    while jid and slept <= max_sleep:
        slept += 5
        sleep(5)
        jid = get_presumed_submitted_or_running(job_list_manager_sge.dag_id)

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs
    job_list_manager_sge._sync()
    new_now = job_list_manager_sge.last_sync
    assert new_now > now
    assert len(job_list_manager_sge.all_error) > 0


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.queue_job(job.job_id)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0

    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_error) > 0


def test_valid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='ls', name='baz',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0
    assert len(job_list_manager.all_done) == 0
    job_list_manager.queue_job(job.job_id)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1

    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_done) > 0


def test_daemon_invalid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="some new job",
                                            name="foobar", num_cores=1))
    job_list_manager_d.queue_job(job.job_id)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, "foobar", partial(
        daemon_invalid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_invalid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_error) == 1


def test_daemon_valid_command(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="ls", name="foobarbaz",
                                            num_cores=1))
    job_list_manager_d.queue_job(job.job_id)

    # Give some time for the job to get to the executor
    timeout_and_skip(3, 30, 1, "foobarbaz", partial(
        daemon_valid_command_check,
        job_list_manager_d=job_list_manager_d))


def daemon_valid_command_check(job_list_manager_d):
    job_list_manager_d._sync()
    return len(job_list_manager_d.all_done) == 1


def test_blocking_update_timeout(job_list_manager_d):
    job = job_list_manager_d.bind_task(Task(command="sleep 3",
                                            name="foobarbaz", num_cores=1))
    job_list_manager_d.queue_job(job)

    with pytest.raises(RuntimeError) as error:
        job_list_manager_d.block_until_any_done_or_error(timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sge_valid_command(job_list_manager_sge_no_daemons):
    job_list_manager_sge = job_list_manager_sge_no_daemons
    job = job_list_manager_sge.bind_task(Task(command="ls",
                                              name="sgefbb",
                                              num_cores=3,
                                              mem_free='6M'))
    job_list_manager_sge.queue_job(job)
    job_list_manager_sge.job_instance_factory.instantiate_queued_jobs()
    job_list_manager_sge._sync()
    assert (job_list_manager_sge.bound_tasks[job.job_id].status ==
            JobStatus.INSTANTIATED)
    print("finishing test_sge_valid_command")


def test_server_502(job_list_manager):
    '''
    GBDSCI-1553

    We should be able to automatically retry if server returns 5XX
    status code. If we exceed retry budget, we should raise informative error
    '''
    err_response = (
        502,
        b'<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body '
        b'bgcolor="white">\r\n<center><h1>502 Bad Gateway</h1></center>\r\n'
        b'<hr><center>nginx/1.13.12</center>\r\n</body>\r\n</html>\r\n'
    )
    good_response = (
        200,
        {'job_dcts': [], 'time': '2019-02-21 17:40:07'}
    )

    job = job_list_manager.bind_task(Task(command='ls', name='baz',
                                          num_cores=1))
    job_list_manager.queue_job(job)
    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # mock requester.get_content to return 2 502s then 200
    with mock.patch('jobmon.client.requester.get_content') as m:
        m.side_effect = [err_response] * 2 + \
            [good_response] + [err_response] * 2

        job_list_manager.get_job_statuses()  # fails at first

        # should have retried twice + one success
        retrier = job_list_manager.requester.send_request.retry
        assert retrier.statistics['attempt_number'] == 3

        # if we end up stopping we should get an error
        with pytest.raises(RuntimeError, match='Status code was 502'):
            retrier.stop = stop_after_attempt(1)
            job_list_manager.get_job_statuses()
