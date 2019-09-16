import os
from functools import partial
from time import sleep

import pytest

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.executors.sge_parameters import SGEParameters
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.models.job import Job
from jobmon.models.executor_parameter_set import ExecutorParameterSet
from tests.timeout_and_skip import timeout_and_skip


@pytest.fixture
def no_daemon(real_dag_id):
    executor = SGEExecutor(project='proj_tools')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False)
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
@pytest.mark.parametrize('mem', ['6G', '6GB', '150MB', '200M'])
@pytest.mark.parametrize('queue', ['all.q'])
def test_new_cluster_with_new_params(real_dag_id, job_list_manager_sge,
                                     mem, queue):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="sge_foobar", executor_class='SGEExecutor',
                m_mem_free=mem,
                num_cores=1,
                queue=queue,
                max_runtime_seconds=600,
                j_resource=False)
    job = job_list_manager_sge.bind_task(task)
    job_list_manager_sge.adjust_resources_and_queue(job)

    timeout_and_skip(step_size=10, max_time=120, max_qw=1,
                     partial_test_function=partial(
                         valid_command_check,
                         job_list_manager_sge=job_list_manager_sge))


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['1TB', '513GB'])
def test_big_memory_adjusted(no_daemon, mem, capsys):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="invalid_memory", m_mem_free=mem, num_cores=8,
                j_resource=True, queue='all.q', max_runtime_seconds=120)
    resources = task.executor_parameters()
    msg = resources.is_valid()[1] # get the ExecutorParameter object
    assert "\n Memory" in msg
    resources.validate()
    assert 512 == resources.m_mem_free


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['125MB', '0GB'])
def test_small_mem_adjusted(no_daemon, mem):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="invalid_memory", m_mem_free=mem, num_cores=8,
                j_resource=True, queue='all.q', max_runtime_seconds=120)
    resources = task.executor_parameters()
    resources.validate()
    assert resources.m_mem_free == 0.128


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['0B', '10gigabytes'])
def test_invalid_mem_adjusted(no_daemon, mem):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="invalid_memory", m_mem_free=mem, num_cores=8,
                j_resource=True, queue='all.q', max_runtime_seconds=120)
    resources = task.executor_parameters()
    resources.validate()
    assert resources.m_mem_free == 1


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['.5TB', '500GB', '500000MB'])
def test_memory_transformed_correctly(mem):
    resource = SGEParameters(mem_free=mem, num_cores=1, queue='all.q',
                             max_runtime_seconds=86400)
    assert resource.m_mem_free == 500


@pytest.mark.cluster
@pytest.mark.parametrize('mem', ['.129GB', '129MB'])
def test_min_memory_transformed_correctly(mem):
    resource = SGEParameters(mem_free=mem, num_cores=1, queue='all.q',
                             max_runtime_seconds=86400)
    assert resource.m_mem_free == 0.129


@pytest.mark.cluster
def test_exclusive_args_both_slots_and_cores(no_daemon):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="exclusive_args_both", m_mem_free='2G', slots=7,
                num_cores=8, j_resource=True, queue='all.q',
                max_runtime_seconds=20)
    resources = task.executor_parameters()
    assert resources.num_cores == 8


@pytest.mark.cluster
def test_exclusive_args_no_slots_or_cores(no_daemon):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="exclusive_args_none", m_mem_free='2G', j_resource=True,
                queue='all.q', max_runtime_seconds=120)
    resources = task.executor_parameters()
    resources.validate()
    assert resources.num_cores == 1


@pytest.mark.cluster
@pytest.mark.parametrize('runtime', [0, -3])
def test_invalid_runtime_caught(no_daemon, runtime):
    task = Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
                name="invalid_runtime", m_mem_free='2G', num_cores=8,
                j_resource=True, queue="all.q", max_runtime_seconds=runtime)
    resources = task.executor_parameters()
    resources.validate()
    assert resources.max_runtime_seconds == (
        24 * 60 * 60)


@pytest.mark.cluster
def test_both_mem_free_error():
    task = BashTask(command="sleep 10", name='test_mem_args',
                    max_attempts=2, mem_free='3G', m_mem_free='2G',
                    num_cores=1, max_runtime_seconds=60)
    resources = task.executor_parameters()
    assert resources.m_mem_free == 2


@pytest.mark.cluster
def test_no_queue_provided(no_daemon):
    job = no_daemon.bind_task(BashTask(command="echo hello", name="sge_foobar",
                                       m_mem_free='1G', num_cores=1,
                                       max_runtime_seconds=600,
                                       j_resource=False))
    sge_executor = no_daemon.job_instance_factory.executor

    no_daemon.adjust_resources_and_queue(job)
    jobs = no_daemon.job_instance_factory._get_jobs_queued_for_instantiation()
    job = jobs[0]

    # the job is setup to run but also check that it has the right qsub cmd
    qsub_cmd = sge_executor._build_qsub_command(
        job.command,
        job.name,
        job.executor_parameters.m_mem_free,
        job.executor_parameters.num_cores,
        job.executor_parameters.queue,
        job.executor_parameters.max_runtime_seconds,
        job.executor_parameters.j_resource,
        job.executor_parameters.context_args,
        sge_executor.stderr,
        sge_executor.stdout,
        sge_executor.project,
        sge_executor.working_dir)
    if 'el7' in os.environ['SGE_ENV']:
        assert 'all.q' in qsub_cmd


def test_sec_exceeds_queue_limit(no_daemon):
    job = no_daemon.bind_task(
        BashTask(command="sleep 10", name='test_mem_args', queue='all.q',
                 max_attempts=2, m_mem_free='3G', num_cores=1,
                 max_runtime_seconds=1382402))
    resources = job.executor_parameters()
    resources.validate()
    assert resources.queue == 'long.q'


def test_sec_exceeds_queue_hard(no_daemon):
    job = no_daemon.bind_task(
        BashTask(command="sleep 10", name='test_mem_args', queue='all.q',
                 max_attempts=2, m_mem_free='3G', num_cores=1,
                 max_runtime_seconds=1382402, hard_limits=True))
    resources = job._task.executor_parameters()
    resources.validate()
    assert resources.queue == 'all.q'
    assert resources.max_runtime_seconds == 259200


def test_mem_exceeds_limit_cant_scale(no_daemon, db_cfg):
    name = "mem_no_scale"
    job = no_daemon.bind_task(
        PythonTask(script=sge.true_path("tests/exceed_mem.py"), name=name,
                   m_mem_free='600M', max_attempts=2, num_cores=1,
                   max_runtime_seconds=40,
                   resource_scales={'max_runtime_seconds': 0.5}))

    no_daemon.adjust_resources_and_queue(job)
    no_daemon.job_instance_factory.instantiate_queued_jobs()

    count = 0
    while get_status(db_cfg, name) != 'A' and count < 20:
        sleep(5)
        count = count + 1
    assert get_status(db_cfg, name) == 'A'
    jobs = no_daemon.get_job_statuses()
    completed, failed, adjusting = no_daemon.parse_adjusting_done_and_errors(jobs)
    if adjusting:
        for task in adjusting:
            task.executor_parameters = partial(no_daemon.adjust_resources,
                                               task)
            no_daemon.adjust_resources_and_queue(task)
    no_daemon.job_instance_factory.instantiate_queued_jobs()

    count = 0
    while get_status(db_cfg, name) != 'F' and count < 10:
        sleep(5)
        count = count + 1
    assert get_status(db_cfg, name) == 'F'

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.job_instances[0].status == 'Z'
        assert job.job_instances[1].status == 'Z'
        assert job.status == 'F'
        resources = DB.session.query(ExecutorParameterSet).filter_by(
                    job_id=job.job_id).all()
        # add checks for increased system resources
        assert job.executor_parameter_set.m_mem_free == 0.6
        assert job.executor_parameter_set.max_runtime_seconds == 60
        DB.session.commit()


def get_status(db_cfg, name):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        DB.session.commit()
        return job.status






