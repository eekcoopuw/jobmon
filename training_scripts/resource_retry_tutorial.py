import uuid

from jobmon.client.api import Tool, ExecutorParameters, BashTask

"""
Instructions:
    This workflow contains one job that will fail once before ultimately
    succeeding. This is to imitate a scenario in which insufficient resources
    are requested, so jobmon scales them after detecting a resource failure,
    and the job is able to succeed.

To Run:
    with jobmon installed in your conda environment from the root of the repo
    run:
    $ python training_scripts/resource_retry_tutorial.py
    Check the database for the workflow called resource_starved_worfklow and
    check the jobs and job instances to observe the expected behavior

Expected Behavior:
    One workflow will be created with one job that has 2 job instances,
    1 that fails with status 'Z' due to resource failure, and one that passes.
    The workflow should therefore succeed
"""


def resource_retry():
    uid = uuid.uuid4()

    tool = Tool.create_tool('resource_retry_tool')
    my_wf = tool.create_workflow(
        name="resource_starved_workflow",
        workflow_args=f"resource_starved_workflow_{uid}")

    # Set the workflow executor
    my_wf.set_executor(executor_class="SGEExecutor", project="ihme_general")

    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than needed
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})

    sleepy_task = BashTask(
        # set sleep to be longer than max runtime, forcing a retry
        "sleep 120",
        # job should succeed on second try. runtime will 150s on try 2
        max_attempts=2,
        executor_parameters=sleepy_params)
    my_wf.add_task(sleepy_task)

    # job will time out and get killed by the cluster. After a few minutes
    # jobmon will notice that it has disappeared and ask SGE for exit status.
    # SGE will show a resource kill. Jobmon will scale memory and runtime by
    # 50% and retry the job at which point it will succeed.
    wfr = my_wf.run()

    if wfr.status == "D":
        print("Done!")
    else:
        raise Exception(f"Workflow failed with status {wfr.status}")


if __name__ == '__main__':
    resource_retry()
