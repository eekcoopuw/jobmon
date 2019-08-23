import pytest
from functools import partial

from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow


def test_dynamic_resource_assignment(real_jsm_jqs, db_cfg):
    task = BashTask(name='dynamic_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=assign_resources)
    wf = Workflow(workflow_args='dynamic_resource_wf')
    wf.add_task(task)
    wf.run()


def assign_resources(*args, **kwargs):
    m_mem_free = '1G'
    max_runtime_seconds = 60
    num_cores = 1
    queue = 'all.q'

    exec_params = ExecutorParameters(m_mem_free=m_mem_free,
                                     max_runtime_seconds=max_runtime_seconds,
                                     num_cores=num_cores, queue=queue)
    return exec_params


def test_static_resource_assignment(real_jsm_jqs):
    executor_parameters = ExecutorParameters(m_mem_free='1G',
                                             max_runtime_seconds=60,
                                             num_cores=1, queue='all.q')
    task = BashTask(name='static_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=executor_parameters)
    wf = Workflow(workflow_args='static_resource_wf')
    wf.add_task(task)
    wf.run()
