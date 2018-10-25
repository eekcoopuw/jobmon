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


@pytest.fixture
def jlm(real_dag_id):
    executor = SGEExecutor(project='proj_jenkins')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


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
@pytest.mark.parametrize('j_resource', [True, False])
@pytest.mark.parametrize('mem_free', ['6G', '6GB', '10MB', '10M', '1T', '1TB'])
@pytest.mark.parametrize('queue', ['all.q', 'long.q', 'profile.q',
                                   'geospatial.q'])
def test_new_cluster_with_new_params(real_dag_id, job_list_manager_sge,
                                     j_resource, mem_free, queue):
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", mem_free=mem_free, num_cores=8,
             j_resource=j_resource,
             queue=queue, max_runtime_seconds=120))
    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


@pytest.mark.cluster
def test_invalid_queues_caught(jlm):
    job = jlm.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="invalid_queue", mem_free='2G', num_cores=8, j_resource=True,
             queue='not_a_real_queue.q', max_runtime_seconds=120))
    jlm.queue_job(job)

    jobs = jlm.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[0])
    assert 'invalid queue' in exc


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['1TB', '1B', '513GB', '10gigabytes'])
def test_invalid_memory_caught(jlm):
    job = jlm.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="invalid_memory", mem_free=mem, num_cores=8, j_resource=True,
             queue='all.q', max_runtime_seconds=120))
    jlm.queue_job(job)

    jobs = jlm.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[0])
    assert ('only request mem_free_gb between 0 and 512GB' in exc
            or 'measure should be an int' in exc)


@pytest.mark.cluster
def test_memory_transformed_correctly(jlm):
    pass


@pytest.mark.cluster
def test_exclusive_args_enforced(jlm):
    job1 = jlm.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="exclusive_args_both", mem_free='2G', multi_slot=8,
             num_cores=8, j_resource=True,
             queue='all.q', max_runtime_seconds=120))
    job2 = jlm.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="exclusive_args_none", mem_free='2G', j_resource=True,
             queue='all.q', max_runtime_seconds=120))
    jlm.queue_job(job2)

    jobs = jlm.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[0])
    assert 'Cannot specify BOTH slots and num_cores' in exc

    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[2])
    assert 'Must pass one of [slots, num_cores]' in exc


@pytest.mark.cluster
def test_exhaustive_args_enforced(jlm):
    # make sure all args are present by cluster. Make sure good error is raised
    pass


@pytest.mark.cluster
@pytest.mark.parametrize('runtime', [(86500: 'all.q'), (604900: 'long.q'),
                                     (604900: 'profile.q'),
                                     (604900, 'geospatial')])
def test_invalid_runtime_by_queue_caught(jlm, runtime):
    job = jlm.bind_task(
    Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
         name="invalid_runtime_{}".format(runtime[1]), mem_free='2G',
         multi_slot=8, num_cores=8, j_resource=True,
         queue=runtime[1], max_runtime_seconds=runtime[0]))
    jlm.queue_job(job)

    jobs = jlm.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[0])
    assert 'Can only run for up to ' in exc


@pytest.mark.cluster
def test_runtime_transformed_correctly(jlm):
    pass


@pytest.mark.cluster
def test_invalid_j_resource_caught(jlm)
    job = jlm.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="j_resource", mem_free='2G', multi_slot=8, num_cores=8,
             j_resource='Nope', queue='all.q', max_runtime_seconds=120))
    jlm.queue_job(job)

    jobs = jlm.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        jlm.job_instance_factory._create_job_instance(jobs[0])
    assert 'j_resource is a bool arg.' in exc
