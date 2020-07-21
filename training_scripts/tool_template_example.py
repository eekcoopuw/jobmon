import os
import sys
import getpass
from jobmon.client.api import Tool, ExecutorParameters


def tool_template_example():
    """
    Instraction:
        This example workflow consists from 3 phases. (Transform, Aggregate, Summarize)
        The flow in this example is:
        1. create tool and workfow
        2. create task template
        3. define executor parameters for task
        4. create task by specifying task template (which is created at step 2)
        5. add tasks to workflow
        6. run workflow

    To Run:
        with jobmon installed in your conda environment from the root of the repo, run:
           $ python training_scripts/tool_template_example.py
    """

    # define some dummy variables for testing
    locations = list(range(10)) # dummy data
    sexs = list(range(2))       # dummy data
    location_hierarchy_id = 0   # dummy data
    user = getpass.getuser()

    # create a tool, workflow and set executor
    jobmon_tool = Tool.create_tool(name="jobmon_testing_tool")
    workflow = jobmon_tool.create_workflow(name="jobmon_workflow")
    workflow.set_executor(
        executor_class="SGEExecutor",
        project="proj_scicomp"
    )

    # create template
    """
    There is only one summarize job that will take the whole hierarchy of locations
    and write a file for each location. Therefore the number of nodes created in the dag
    will not be dictated by location hierarchy id, but the script will need that information
    to create the correct output, therefore location hierarchy is not a node are, it is a task arg
    """
    template_transform = jobmon_tool.get_task_template(
        template_name = "transform",
        command_template = "{python} {script} --location_id {location_id} --sex_id {sex_id} --output_file_path {output_file_path}",
        node_args = ["location_id", "sex_id"],
        task_args = ["output_file_path"],
        op_args = ["python", "script"]
    )
    template_aggregate = jobmon_tool.get_task_template(
        template_name = "aggregate",
        command_template = "{python} {script} --location_id {location_id} --output_file_path {output_file_path}",
        node_args = ["location_id"],
        task_args = ["output_file_path"],
        op_args = ["python", "script"]
    )
    template_summarize = jobmon_tool.get_task_template(
        template_name = "summarize",
        command_template = "{python} {script} --location_hierarchy_id {location_hierarchy_id} --output_file_path {output_file_path}",
        node_args = [],
        task_args = ["location_hierarchy_id", "output_file_path"],
        op_args = ["python", "script"]
    )

    # set executor parameters
    executor_parameters_transform = ExecutorParameters(
        m_mem_free = "1G",
        num_cores = 1,
        queue = "all.q",
        max_runtime_seconds = 60
    )
    executor_parameters_aggregate = ExecutorParameters(
        m_mem_free = "2G",
        num_cores = 2,
        queue = "long.q",
        max_runtime_seconds = 120
    )
    executor_parameters_summarize = ExecutorParameters(
        m_mem_free = "3G",
        num_cores = 3,
        queue = "all.q",
        max_runtime_seconds = 180
    )

    # create task
    task_all_list = []
    # tasks for transform phase
    task_transform_by_location = {}
    for location_id in locations:
        task_location_list = []
        for sex_id in sexs:
            task = template_transform.create_task(
                executor_parameters = executor_parameters_transform,
                name = f"transform_{location_id}_{sex_id}",
                upstream_tasks = [],
                max_attempts = 3,
                python = sys.executable,
                script = os.path.abspath(f"/ihme/scratch/users/{user}/script/transform.py"),
                location_id = location_id,
                sex_id = sex_id,
                output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/transform"
            )
            # append task to workflow and the list
            task_all_list.append(task)
            task_location_list.append(task)
        # create dictionary by location
        task_transform_by_location[location_id] = task_location_list

    # tasks for aggregate phase
    task_aggregate_list = []
    for location_id in locations:
        upstreams_tasks = task_transform_by_location[location_id]
        task = template_aggregate.create_task(
            executor_parameters = executor_parameters_aggregate,
            name = f"aggregate_{location_id}",
            upstream_tasks = upstreams_tasks,
            max_attempts = 3,
            python = sys.executable,
            script = os.path.abspath(f"/ihme/scratch/users/{user}/script/aggregate.py"),
            location_id = location_id,
            output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/aggregate"
        )
        task_all_list.append(task)
        task_aggregate_list.append(task)

    # tasks for summarize phase
    task = template_summarize.create_task(
        executor_parameters = executor_parameters_summarize,
        name = f"summarize_{location_hierarchy_id}",
        upstream_tasks = task_aggregate_list,
        max_attempts = 1,
        python = sys.executable,
        script = os.path.abspath(f"/ihme/scratch/users/{user}/script/summarize.py"),
        location_hierarchy_id = location_hierarchy_id,
        output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/summarize"
    )
    task_all_list.append(task)

    # add tasks to workflow
    workflow.add_tasks(task_all_list)

    # run workflow
    workflow.run()

if __name__ == '__main__':
    tool_template_example()