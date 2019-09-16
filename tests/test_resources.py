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
    exit = wf.run()
    assert exit == 0 # confirm successful run

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = f"SELECT * from executor_parameter_set join job using(job_id)" \
                f" where dag_id = {wf.dag_id}"
        resp = DB.session.execute(query).fetchall()
        DB.session.commit()
        assert resp[0].max_runtime_seconds == 60
        assert resp[0].m_mem_free == 0.4
        assert resp[0].num_cores == 2


def assign_resources(*args, **kwargs):
    """Callable to be evaluated by task when it is queued to run"""
    m_mem_free = '400M'
    max_runtime_seconds = 60
    num_cores = 2
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



