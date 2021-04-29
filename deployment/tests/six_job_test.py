import uuid

from jobmon.client.workflow import Workflow
from jobmon.client.api import Tool, ExecutorParameters
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate


def six_job_test():
    """
    Creates and runs one workflow with six jobs using template. Used as a template for
    R client
    """
    # tool
    my_tool = Tool.create_tool(name=f'r_example_tool_{uuid.uuid4()}')

    # workflow
    wf = Workflow(tool_version_id=my_tool.active_tool_version_id, name=f"six-job-test-{uuid.uuid4()}")

    # Set the executor
    wf.set_executor(executor_class='SGEExecutor', project="ihme_general")

    # Define executor parameters for our tasks
    params = ExecutorParameters(num_cores=1, m_mem_free="1G", queue='all.q', max_runtime_seconds=100)

    # Define task template
    sleep_template = my_tool.get_task_template(template_name='sleep_template1',
                                               command_template='sleep {sleep_time}',
                                               node_args=['sleep_time'])

    # First Tier
    # Deliberately put in on the long queue with max runtime > 1 day
    t1 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_1',
                                    sleep_time=["1"])

    # Second Tier, both depend on first tier
    t2 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_2',
                                    sleep_time=["20"],
                                    upstream_tasks=[t1])
    t3 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_3',
                                    sleep_time=["25"],
                                    upstream_tasks=[t1])

    # Third Tier, cross product dependency on second tier
    t4 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_4',
                                    sleep_time=["10"],
                                    upstream_tasks=[t2, t3])
    t5 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_5',
                                    sleep_time=["13"],
                                    upstream_tasks=[t2, t3])

    # Fourth Tier, ties it all back together
    t6 = sleep_template.create_task(executor_parameters=params,
                                    name='sleep_6',
                                    sleep_time=["19"],
                                    upstream_tasks=[t4, t5])

    wf.add_tasks([t1, t2, t3, t4, t5, t6])
    print("Running the workflow, about 70 seconds minimum")
    wf.run()
    print("All good, dancing pixies.")


if __name__ == "__main__":
    six_job_test()
