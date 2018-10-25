import pytest
import sys
import os
import os.path as path


from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task

from tests.timeout_and_skip import timeout_and_skip

if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


@pytest.mark.cluster
def test_new_cluster_request(real_dag_id, job_list_manager_sge):
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", mem_free='6G', num_cores=8, queue='all.q',
             max_runtime_seconds=120))
    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


def valid_command_check(job_list_manager_sge):
    job_list_manager_sge._sync()
    if len(job_list_manager_sge.all_done) == 1:
        # Success
        return True
    else:
        return False


@pytest.mark.cluster
@pytest.mark.parametrize('archive', [True, False])
@pytest.mark.parametrize('mem_free', ['6G', '6GB', '10MB', '10M', '1T', '1TB'])
@pytest.mark.parametrize('queue', ['all.q', 'long.q', 'profile.q'])
def test_new_cluster_with_new_params(real_dag_id, job_list_manager_sge,
                                     archive, mem_free, queue):
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", mem_free=mem_free, num_cores=8, archive=True,
             queue=queue, max_runtime_seconds=120))
    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


@pytest.mark.cluster
def test_invalid_queues_caught(real_dag_id, job_list_manager_sge):
    # how do we do a pytest.raises() when the raise will be from the
    # SGEExecutor on the node?
    pass


@pytest.mark.cluster
def test_invalid_queues_caught(real_dag_id, job_list_manager_sge):
    # how do we do a pytest.raises() when the raise will be from the
    # SGEExecutor on the node?
    pass


@pytest.mark.cluster
def test_invalid_memory_caught(real_dag_id, job_list_manager_sge):
    pass


@pytest.mark.cluster
def test_exclusive_args_enforced(real_dag_id, job_list_manager_sge):
    pass


@pytest.mark.cluster
def test_exhaustive_args_enforced(real_dag_id, job_list_manager_sge):
    # make sure all args are present by cluster. Make sure good error is raised
    pass


@pytest.mark.cluster
def test_invalid_runtime_by_queue_caught(real_dag_id, job_list_manager_sge):
    pass


@pytest.mark.cluster
def test_invalid_j_resource_caught(real_dag_id, job_list_manager_sge)
    pass
