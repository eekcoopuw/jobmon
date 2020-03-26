import ast
import os
import pytest

from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.exceptions import CallableReturnedInvalidObject
from jobmon.models.workflow_run_status import WorkflowRunStatus


this_dir = os.path.dirname(os.path.abspath(__file__))
resource_file = os.path.join(this_dir, 'resources.txt')


def test_callable_returns_exec_params(db_cfg, client_env):
    """Test when the provided callable returns the correct parameters"""
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
