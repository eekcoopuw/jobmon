import sys

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.executors.base import ExecutorParameters


"""
Instructions:
    The first workflow has two tasks, the first one will succeed, and the 
    second one will fail multiple times so the workflow as a whole will fail
    
    The second time around the identical workflow will be created, and the 
    same tasks will be added but the "bug" in the second task will be fixed 
    so that it has enough time to run successfully and pass.
    
To Run from the root of the repo: 
    1) Choose a string to assign to be a unique identifier for your workflows 
       (ex. scicomp_training)
    2) With jobmon installed in your conda environment run:
       $ python -c 'from training_scripts import resume_tutorial; resume_tutorial.first_wf_run()' <identifier>
    3) Check the jobmon database to make sure the expected behavior occurred
    4) Run:
       $ python -c 'from training_scripts import resume_tutorial; resume_tutorial.wf_rerun_to_succeed()' <identifier>

Expected Behavior:
    1 Dag ID created, 1 workflow created called 'resume_tutorial', 2 jobs 
    created. 1 Job Instance for the 1st job, 2 job instances for the second 
    job. Workflow Fails.
    
    Upon rerun: No new dag or workflow created, 1 new workflow run created 
    with same old workflow id, only the second job is rerun, so no new 
    job_instances will be created for the first job. Workflow Succeeds.
"""


def first_wf_run():
    identifier = 'foo'
    if len(sys.argv) > 1:
        identifier = sys.argv[1]
    # create workflow
    workflow1 = Workflow(name=f"resume_tutorial_{identifier}",
                         workflow_args=f"resume_tutorial_{identifier}",
                         project='proj_tools')

    # create task 1 parameters
    succeed_task_params = ExecutorParameters(
        max_runtime_seconds=60, m_mem_free='1G',
        resource_scales={'max_runtime_seconds': 0.1, 'm_mem_free': 0.5})

    # create task 1
    succeed_task = BashTask(name='succeed_task', command='sleep 5',
                            executor_parameters=succeed_task_params,
                            max_attempts=2)

    # create task 2 parameters with insufficient time for task 2 to complete
    fail_task_params = ExecutorParameters(
        max_runtime_seconds=5, m_mem_free='1G',
        resource_scales={'max_runtime_seconds': 0.1, 'm_mem_free': 0.5})
    # create task 2
    fail_task = BashTask(name='fail_task', command='sleep 10',
                         executor_parameters=fail_task_params,
                         max_attempts=2)
    # add tasks to workflow
    workflow1.add_tasks([succeed_task, fail_task])
    # run workflow
    workflow1.run()


def wf_rerun_to_succeed():
    identifier = 'foo'
    if len(sys.argv) > 1:
        identifier = sys.argv[1]
    # recreate identical workflow with resume set to True
    workflow2 = Workflow(name=f"resume_tutorial_identifier",
                         workflow_args=f"resume_tutorial_{identifier}",
                         project='proj_tools', resume=True)

    # same task 1 parameters as before
    succeed_task_params = ExecutorParameters(
        max_runtime_seconds=60, m_mem_free='1G', num_cores=1,
        resource_scales={'max_runtime_seconds': 0.1, 'm_mem_free': 0.5})
    # same task 1
    succeed_task = BashTask(name='succeed_task', command='sleep 5',
                            executor_parameters=succeed_task_params,
                            max_attempts=2)

    # updated task 2 parameters so it will have time to run completely
    fail_task_params = ExecutorParameters(
        max_runtime_seconds=30, m_mem_free='1G', num_cores=1,
        resource_scales={'max_runtime_seconds': 0.1, 'm_mem_free': 0.5})
    # same task 2
    fail_task = BashTask(name='fail_task', command='sleep 10',
                         executor_parameters=fail_task_params,
                         max_attempts=2)

    # add tasks to workflow (jobmon will recognize that this workflow
    # already exists and resume the workflow instead of starting from the
    # beginning)
    workflow2.add_tasks([succeed_task, fail_task])
    workflow2.run()
