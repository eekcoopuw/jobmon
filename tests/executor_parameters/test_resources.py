import ast
import os

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


def test_validate(db_cfg, client_env):
    """ Test that adjust and validate work as expected."""
    from jobmon.client.task_resources import TaskResources
    from jobmon.constants import TaskResourcesType
    from jobmon.client.cluster import Cluster

    cluster = Cluster.get_cluster("multiprocess")

    # Create a valid resource. In test_utils.db_schema, note that min/max cores for the multiprocess
    # cluster null.q is 1/20
    happy_resource: TaskResources = cluster.create_valid_task_resources(
        resource_params={'cores': 10, 'queue': 'null.q'},
        task_resources_type_id=TaskResourcesType.VALIDATED,
        fail=False
    )

    assert happy_resource.concrete_resources.resources['cores'] == 10
    assert happy_resource.queue.queue_name == "null.q"

    # Create invalid resource
    # Try a fail call first
    with pytest.raises(ValueError):
        cluster.create_valid_task_resources(
            resource_params={'cores': 100, 'queue': 'null.q'},
            task_resources_type_id=TaskResourcesType.VALIDATED,
            fail=True
        )

    # Same call but check that the resources are coerced
    unhappy_resource: TaskResources = cluster.create_valid_task_resources(
        resource_params={'cores': 100, 'queue': 'null.q'},
        task_resources_type_id=TaskResourcesType.VALIDATED,
        fail=False)
    assert unhappy_resource.concrete_resources.resources['cores'] == 20


def test_swarmtask_resources_integration(db_cfg, client_env):
    """ Check that taskresources defined in task are passed to swarmtask appropriately"""
    from jobmon.constants import TaskResourcesType
    from jobmon.client.tool import Tool

    tool = Tool()
    wf = tool.create_workflow(default_cluster_name='multiprocess')
    tt = tool.get_task_template(template_name='foo', command_template='bar')

    # Create tasks
    task = tt.create_task(compute_resources={'cores': 10, 'queue': 'null.q'}, resource_scales={'cores': .5}, cluster_name='multiprocess')

    # Add to workflow, bind and create wfr
    wf.add_task(task)
    wf.bind()
    wfr = wf._create_workflow_run()

    # Check swarmtask resources
    swarmtask = wfr.swarm_tasks[task.task_id]
    initial_resources = swarmtask.get_task_resources()
    assert initial_resources.concrete_resources.resources == {'cores': 10}
    assert initial_resources.task_resources_type_id == TaskResourcesType.VALIDATED

    # Call adjust. Multiprocess doesn't implement adjust, but the path should work and return an adjusted task resource
    wfr._adjust_resources(swarmtask)
    assert len(swarmtask.bound_parameters) == 2
    scaled_params = swarmtask.bound_parameters[-1]
    assert scaled_params.task_resources_type_id == TaskResourcesType.ADJUSTED
    assert scaled_params.concrete_resources.resources == {'cores': 10}  # No scaling implemented
