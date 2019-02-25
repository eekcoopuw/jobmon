import pytest

from jobmon.client.swarm.executors.sge_resource import SGEResource
from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.exceptions import InvalidResponse

from tests.timeout_and_skip import timeout_and_skip

from functools import partial


@pytest.fixture
def no_daemon(real_dag_id):
    executor = SGEExecutor(project='proj_jenkins')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


def valid_command_check(job_list_manager_sge):
    job_list_manager_sge._sync()
    if len(job_list_manager_sge.all_done) == 1:
        # Success
        return True
    else:
        return False


@pytest.mark.cluster
# @pytest.mark.parametrize('j_resource', [True, False])
@pytest.mark.parametrize('mem_free', ['6G', '6GB', '10MB', '10M'])
@pytest.mark.parametrize('queue', ['all.q'])
# add these queues once they have actually been created on the fair cluster:
# 'long.q', 'profile.q','geospatial.q'])
def test_new_cluster_with_new_params(real_dag_id, job_list_manager_sge,
                                     mem_free, queue):

    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", mem_free=mem_free, num_cores=1, queue=queue,
             max_runtime_seconds=600, j_resource=False))

    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


@pytest.mark.cluster
def test_invalid_queues_caught(no_daemon):
    job = no_daemon.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="invalid_queue", mem_free='2G', num_cores=8, j_resource=True,
             queue='not_a_real_queue.q', max_runtime_seconds=120))
    no_daemon.queue_job(job)

    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        no_daemon.job_instance_factory._create_job_instance(jobs[0])
    assert 'Got invalid' in exc.value.args[0]


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['1TB', '1B', '513GB', '10gigabytes'])
def test_invalid_memory_caught(no_daemon, mem):
    job = no_daemon.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="invalid_memory", mem_free=mem, num_cores=8, j_resource=True,
             queue='all.q', max_runtime_seconds=120))
    no_daemon.queue_job(job)

    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        no_daemon.job_instance_factory._create_job_instance(jobs[0])
    assert ('only request mem_free_gb between 0 and 512GB' in exc.value.args[0]
            or 'measure should be an int' in exc.value.args[0])


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['.5TB', '500GB', '500000MB'])
def test_memory_transformed_correctly(mem):
    resource = SGEResource(mem_free=mem, num_cores=1, queue='all.q',
                           max_runtime_seconds=86400)
    mem_free_gb = resource._transform_mem_to_gb()
    assert mem_free_gb == 500


@pytest.mark.cluster
def test_exclusive_args_both_slots_and_cores(no_daemon):
    job = no_daemon.bind_task(Task(
            command=sge.true_path("tests/shellfiles/jmtest.sh"),
            name="exclusive_args_both", mem_free='2G', slots=8,
            num_cores=8, j_resource=True, queue='all.q',
            max_runtime_seconds=120))
    no_daemon.queue_job(job)

    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        no_daemon.job_instance_factory._create_job_instance(jobs[0])
    assert 'Cannot specify BOTH slots and num_cores' in exc.value.args[0]

@pytest.mark.cluster
def test_exclusive_args_no_slots_or_cores(no_daemon):
    job = no_daemon.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="exclusive_args_none", mem_free='2G', j_resource=True,
             queue='all.q', max_runtime_seconds=120))
    no_daemon.queue_job(job)

    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        no_daemon.job_instance_factory._create_job_instance(jobs[0])
    assert 'Must pass one of [slots, num_cores]' in exc.value.args[0]


@pytest.mark.skip("New cluster queues are not up yet")
@pytest.mark.cluster
@pytest.mark.parametrize('runtime', [(86500, 'all.q'), (604900, 'long.q'),
                                     (604900, 'profile.q'),
                                     (604900, 'geospatial')])
def test_invalid_runtime_by_queue_caught(no_daemon, runtime):
    job = no_daemon.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="invalid_runtime_{}".format(runtime[1]), mem_free='2G',
             num_cores=8, j_resource=True, queue=runtime[1],
             max_runtime_seconds=runtime[0]))
    no_daemon.queue_job(job)

    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    with pytest.raises(ValueError) as exc:
        no_daemon.job_instance_factory._create_job_instance(jobs[0])
    assert 'Can only run for up to ' in exc.value.args[0]


@pytest.mark.cluster
def test_runtime_transformed_correctly():
    resource = SGEResource(mem_free='2G', num_cores=1, queue='all.q',
                           max_runtime_seconds=86399)
    hms = resource._transform_secs_to_hms()
    # turns into '23:59:59', if it is exactly 86400 it becomes 1 day 0:00:00
    h, m, s = hms.split(":")
    assert int(h) == 23
    assert int(m) == 59
    assert int(s) == 59

@pytest.mark.skip("erroring out before it gets to validation because of"
                  " non-bool type")
@pytest.mark.cluster
def test_invalid_j_resource_caught(no_daemon):
    # errors out way before SGEExecutor validation because it is not a
    # boolean type
    with pytest.raises(InvalidResponse) as exc:
        job = no_daemon.bind_task(
            Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                 name="j_resource", mem_free='2G', num_cores=8,
                 j_resource='Nope', queue='all.q', max_runtime_seconds=120))

    assert 'Could not create_job' in exc.value.args[0]


@pytest.mark.cluster
def test_both_mem_free_error():
    from jobmon.client.swarm.workflow.bash_task import BashTask
    expected_msg = ("Cannot pass both mem_free: 1G and m_mem_free: 1G when "
                    "creating a task. mem_free is deprecated, so it's "
                    "recommended to use m_mem_free.")

    with pytest.raises(ValueError) as error:
        test = BashTask(command="sleep 10", name='test_mem_args',
                        mem_free='1G', m_mem_free='1G', max_attempts=2,
                        slots=1, max_runtime_seconds=60)

    assert expected_msg == str(error.value)
