import getpass
import uuid
import os

from jobmon.client.api import Tool, ExecutorParameters, PythonTask

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
    retry_tool = Tool.create_tool('retry_tool')
    wf = retry_tool.create_workflow(
        name="workflow_with_many_retries",
        workflow_args=f"workflow_with_many_retries_{uid}")

    wf.set_executor(executor_class="SGEExecutor", project="ihme_general")

    params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})

    name = "retry_task"
    output_file_name = f"/ihme/scratch/users/{user}/retry_output"
    current_path = os.path.abspath(__file__)
    script_path = os.path.join(os.path.dirname(current_path), '../../tests/_scripts')
    assert os.path.isdir(script_path)
    retry_task = PythonTask(
        script=os.path.join(script_path, "remote_sleep_and_write.py"),
        args=["--sleep_secs", "4",
              "--output_file_path", output_file_name,
              "--fail_count", 3,
              "--name", name],
        name=name,
        max_attempts=4,
        executor_parameters=params)

    wf.add_task(retry_task)

    # 3 job instances will fail before ultimately succeeding
    wfr = wf.run()

    if wfr.status != "D":
        raise RuntimeError(
            "The workflow failed to run, look for errors ",
            f"associated with task_id {retry_task.task_id}")


if __name__ == '__main__':
    jobs_that_retry()
