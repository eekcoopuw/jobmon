import getpass
import uuid
from jobmon import Workflow, PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.executors import sge_utils

"""
Instructions:
    This workflow contains one job that will fail three times before ultimately 
    succeeding. This is to imitate a scenario in which a job might die randomly 
    from cluster events, not because it contains a bug.
    
To Run:
    with jobmon installed in your conda environment from the root of the repo 
    run:
    $ python training_scripts/retry_tutorial.py
    Check the database for the workflow called workflow_with_many_retries and 
    check the jobs and job instances to observe the expected behavior 
    
Expected Behavior:
    One workflow will be created with one job that has 4 job instances, 
    3 that fail and the last one succeeds. The workflow should therefore 
    succeed
"""


def jobs_that_retry():
    user = getpass.getuser()
    uid = uuid.uuid4()

    # create workflow
    wf = Workflow(
        name="workflow_with_many_retries",
        workflow_args=f"workflow_with_many_retries_{uid}",
        project="proj_tools")

    params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})

    name = "retry_task"
    output_file_name = f"/ihme/scratch/users/{user}/retry_output"
    retry_task = PythonTask(
        script=sge_utils.true_path("tests/remote_sleep_and_write.py"),
        args=["--sleep_secs", "4",
              "--output_file_path", output_file_name,
              "--fail_count", 3,
              "--name", name],
        name=name, max_attempts=4, executor_parameters = params)

    wf.add_task(retry_task)

    # 3 job instances will fail before ultimately succeeding
    wf.run()


if __name__ == '__main__':
    jobs_that_retry()
