import ast
import os

from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import CallableReturnedInvalidObject

import pytest

this_dir = os.path.dirname(os.path.abspath(__file__))
resource_file = os.path.join(this_dir, 'resources.txt')


def test_callable_returns_exec_params(db_cfg, client_env):
    """Test when the provided callable returns the correct parameters"""
    from jobmon.client.distributor.strategies import sge  # noqa: F401
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    def resource_file_does_exist(*args, **kwargs):
        # file contains dict with
        # {'m_mem_free': '2G', 'max_runtime_seconds': 30, 'num_cores': 1,
        # 'queue': 'all.q'}
        with open(resource_file, "r") as file:
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

    task = BashTask(name='good_callable_task', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=resource_file_does_exist,
                    executor_class="SequentialExecutor")
    workflow = UnknownWorkflow(workflow_args='dynamic_resource_wf_good_file',
                               executor_class="SequentialExecutor")
    workflow.add_task(task)
    workflow.run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
                SELECT * FROM executor_parameter_set
                JOIN task on task.id = executor_parameter_set.task_id
                WHERE task.name = 'good_callable_task'
                AND parameter_set_type = 'V' """
        exec_params = DB.session.execute(query).fetchall()
        DB.session.commit()
        for params in exec_params:
            assert params['max_runtime_seconds'] == 30
            assert params['m_mem_free'] == 2
            assert params['num_cores'] == 1


def test_callable_fails_bad_filepath(db_cfg, client_env):
    """test that an exception in the callable gets propagated up the call stack
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    def resource_filepath_does_not_exist(*args, **kwargs):
        fp = os.path.join(this_dir, 'file_that_does_not_exist.txt')
        file = open(fp, "r")
        file.read()

    task = BashTask(name='bad_callable_wrong_file', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=resource_filepath_does_not_exist,
                    executor_class="SequentialExecutor")
    wf = UnknownWorkflow(workflow_args='dynamic_resource_wf_bad_file',
                         executor_class="SequentialExecutor")
    wf.add_task(task)
    with pytest.raises(FileNotFoundError):
        wf.run()


def test_callable_returns_wrong_object(db_cfg, client_env):
    """test that the callable cannot return an invalid object"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    def wrong_return_params(*args, **kwargs):
        wrong_format = ['1G', 60, 1]
        return wrong_format

    task = BashTask(name='bad_callable_wrong_return_obj', command='sleep 1',
                    max_attempts=2,
                    executor_parameters=wrong_return_params,
                    executor_class="SequentialExecutor")
    wf = UnknownWorkflow(workflow_args='dynamic_resource_wf_wrong_param_obj',
                         executor_class="SequentialExecutor")
    wf.add_task(task)
    with pytest.raises(CallableReturnedInvalidObject):
        wf.run()


def test_static_resource_assignment(db_cfg, client_env):
    """test that passing in executor parameters object works as expected"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    executor_parameters = ExecutorParameters(
                    m_mem_free='1G', max_runtime_seconds=60,
                    num_cores=1, queue='all.q',
                    executor_class="SequentialExecutor")
    task = BashTask(name='static_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=executor_parameters,
                    executor_class="SequentialExecutor")
    workflow = UnknownWorkflow(workflow_args='static_resource_wf',
                               executor_class="SequentialExecutor")
    workflow.add_task(task)
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
                SELECT * FROM executor_parameter_set
                JOIN task on task.id = executor_parameter_set.task_id
                WHERE task.name = 'static_resource_task'
                AND parameter_set_type = 'V' """
        exec_params = DB.session.execute(query).fetchall()
        DB.session.commit()
        for params in exec_params:
            assert params['max_runtime_seconds'] == 60
            assert params['m_mem_free'] == 1
            assert params['num_cores'] == 1
            assert params['queue'] == "all.q"


def test_executor_parameter_copy(db_cfg, client_env):
    """test that 1 executorparameters object passed to multiple tasks are distinct objects,
    and scaling 1 task does not scale the others"""
    from jobmon.client.api import BashTask, UnknownWorkflow
    from jobmon.client.swarm.swarm_task import SwarmTask
    from jobmon.client.distributor.strategies.sge import SGEExecutor  # noqa F401
    from math import isclose

    # Use SGEExecutor for adjust methods, but the executor is never called
    # Therefore, not an SGEIntegration test
    executor_parameters = ExecutorParameters(
        m_mem_free='1G', max_runtime_seconds=60,
        num_cores=1, queue='all.q',
        executor_class="SGEExecutor",
        resource_scales={'max_runtime_seconds': 0.5, 'm_mem_free': 0.5})

    task1 = BashTask(name="foo", command="echo foo", executor_parameters=executor_parameters,
                     executor_class="SGEExecutor")
    task2 = BashTask(name="bar", command="echo bar", executor_parameters=executor_parameters,
                     executor_class="SGEExecutor")

    # Ensure memory addresses are different
    assert id(executor_parameters) != id(task1.executor_parameters)
    assert id(executor_parameters) != id(task2.executor_parameters)
    assert id(task1.executor_parameters) != id(task2.executor_parameters)

    # Create a workflow to bind the tasks
    workflow = UnknownWorkflow(executor_class="SGEExecutor")
    workflow.add_tasks([task1, task2])
    workflow.bind()

    task1.workflow_id = workflow.workflow_id
    task2.workflow_id = workflow.workflow_id

    task1.bind()
    task2.bind()

    # Create swarm tasks
    swarmtask1 = SwarmTask(
        task_id=task1.task_id,
        status=task1.initial_status,
        task_args_hash=task1.task_args_hash,
        executor_parameters=task1.executor_parameters,
        max_attempts=task1.max_attempts)

    swarmtask2 = SwarmTask(
        task_id=task2.task_id,
        status=task2.initial_status,
        task_args_hash=task2.task_args_hash,
        executor_parameters=task2.executor_parameters,
        max_attempts=task2.max_attempts)

    # Adjust task 1
    swarmtask1.bound_parameters.append(swarmtask1.get_executor_parameters())
    adjusted_params = swarmtask1.adjust_resources(swarmtask1)
    assert isclose(adjusted_params.m_mem_free, 1.5)  # Scaled by 1.5
    assert isclose(swarmtask2.get_executor_parameters().m_mem_free, 1.0)  # Unscaled


@pytest.mark.qsubs_jobs
def test_resource_arguments(db_cfg, client_env):
    """
    Test the parsing/serialization max run time and cores.
    90,000 seconds is deliberately longer than one day, testing a specific
    bug"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    t1 = BashTask("sleep 10",
                  queue='all.q',
                  max_runtime_seconds=90_000,
                  num_cores=2,
                  executor_class="SequentialExecutor")
    wf = UnknownWorkflow("test_resource_arguments",
                         executor_class="SequentialExecutor")
    wf.add_tasks([t1])
    wfr = wf.run()
    assert wfr.status == WorkflowRunStatus.DONE


def test_adjust_validate(db_cfg, client_env):
    """ Test that adjust and validate work as expected.

    For queue hopping, note in queries.py that all.q has a max runtime of 72 hours
    but long.q has a max runtime of 384 hours.

    """
    from jobmon.client.task_resources import TaskResources
    from jobmon.client.task import Task
    from jobmon.client.swarm.swarm_task import SwarmTask

    initial_queue = 'all.q'
    fallback_queues = ['long.q']
    t1 = Task(task_resources={'m_mem_free': '1G', 'max_runtime_seconds': 60 * 3600, 'queue': initial_queue,
        'fallback_queues': fallback_queues})
    t1.workflow_id = 1  # arbitrary
    t1.bind()

    st1 = SwarmTask(task_id=t1.id, status="G", task_args_hash=t1.task_args_hash, task_resources=t1.task_resources)

    st1.validate(fail=True)  # Should pass validation
    st1.adjust_resources()  # New params: m_mem_free = 1.5G, max_runtime_seconds = 60 * 3600 * 1.5, queue = long.q
    taskresources_1 = st1.task_resources
    assert taskresources_1.queue.queue_name == fallback_queues[0]

    # Try another adjust where runtime exceeds all available queues,
    # and check that the values are set to the right maximums
    taskresources_1.resource_scales = {'max_runtime_seconds': 3000}
    new_resources = TaskResources.adjust(taskresources_1, only_scale=['max_runtime_seconds'])

    assert new_resources.queue.queue_name == fallback_queues[0]  # No hopping
    # Check that runtime is maxed
    assert new_resources.requested_resources['max_runtime_seconds'] == new_resources.queue.parameters['runtime'][1]
    # Check that memory is unchanged
    assert new_resources.requested_resources['m_mem_free'] == taskresources_1.requested_resources['m_mem_free']
