import ast
import json
import pytest
from functools import partial
from time import sleep

from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.exceptions import CallableReturnedInvalidObject


def test_callable_returns_exec_params(real_jsm_jqs, db_cfg):
    """Test when the provided callable returns the correct parameters"""
    task = BashTask(name='good_callable_task', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=resource_file_does_exist)
    wf = Workflow(workflow_args='dynamic_resource_wf_good_file')
    wf.add_task(task)
    wf.run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """SELECT * FROM executor_parameter_set JOIN job using(job_id) 
                WHERE job.name = 'good_callable_task'
                AND parameter_set_type = 'V' """
        exec_params = DB.session.execute(query).fetchall()
        DB.session.commit()
        for params in exec_params:
            assert params['max_runtime_seconds'] == 30
            assert params['m_mem_free'] == 2
            assert params['num_cores'] == 1


def resource_file_does_exist(*args, **kwargs):
    # file contains dict with
    # {'m_mem_free': '2G', 'max_runtime_seconds': 30, 'num_cores': 1,
    # 'queue': 'all.q'}
    fp = '/ihme/scratch/users/svcscicompci/tests/jobmon/resources.txt'
    with open(fp, "r") as file:
        resources = file.read()
        resource_dict = ast.literal_eval(resources)
    m_mem_free = resource_dict['m_mem_free']
    max_runtime_seconds = int(resource_dict['max_runtime_seconds'])
    num_cores = int(resource_dict['num_cores'])
    queue = resource_dict['queue']

    params = ExecutorParameters(m_mem_free=m_mem_free, num_cores=num_cores,
                                queue=queue,
                                max_runtime_seconds=max_runtime_seconds)

    return params


def test_callable_fails_bad_filepath(real_jsm_jqs):
    task = BashTask(name='bad_callable_wrong_file', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=resource_filepath_does_not_exist)
    wf = Workflow(workflow_args='dynamic_resource_wf_bad_file')
    wf.add_task(task)
    with pytest.raises(FileNotFoundError):
        wf.run()


def resource_filepath_does_not_exist(*args, **kwargs):
    fp = '/ihme/scratch/users/svcscicompci/file_that_does_not_exist.txt'
    file = open(fp, "r")
    resources = file.read()
    resource_dict = json.loads(resources)


def test_callable_returns_wrong_object(real_jsm_jqs):
    task = BashTask(name='bad_callable_wrong_return_obj', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=wrong_return_params)
    wf = Workflow(workflow_args='dynamic_resource_wf_wrong_param_obj')
    wf.add_task(task)
    with pytest.raises(CallableReturnedInvalidObject):
        wf.run()


def wrong_return_params(*args, **kwargs):
    wrong_format = ['1G', 60, 1]
    return wrong_format


def test_static_resource_assignment(real_jsm_jqs):
    executor_parameters = ExecutorParameters(m_mem_free='1G',
                                             max_runtime_seconds=60,
                                             num_cores=1, queue='all.q')
    task = BashTask(name='static_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=executor_parameters)
    wf = Workflow(workflow_args='static_resource_wf')
    wf.add_task(task)
    wf.run()


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
                f" WHERE dag_id = {wf.dag_id} AND name = 'dynamic_resource_task'"
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


