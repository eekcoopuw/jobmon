import getpass
import os
import sys
import uuid

from jobmon.client.tool import Tool

"""
TODO
1. Add injectable variables for IHME specific file paths and URLs
2. Replicate this file in the training repo? 
3. R versions

Instructions:

  This workflow is similar to many IHME modelling workflows, with a class three-phase fork-and-join task flow:
  1. A data preparation phase, with one job. In a real modelling pipeline this would split a large input file
     input file into manageable pieces and clean the data. In our example this task generates dummy
     intermediate files for the next phase.
  2. A broad phase with one Task per (dummy) location_id.
  3. A summarization phase consisting of a single Task that reads the intermediate results from the individual
     location models.

  The steps in this example are:
  1. Create a tool
  2. Create a workflow using the tool from step 1
  3. Create three task templates using the tool from step 1
     a. Template for the Data Prep Task
     b. Template for the separate location tasks
     c. Template for the Summarization Phase
  4. Create tasks using the templates from step 3
     a. Add the necessary edge dependencies
  5. Add created tasks to the workflow
  6. Run the workflow

To actually run the provided example:
  Make sure Jobmon is installed in your activated conda environment, and that you're on
  the Slurm cluster in a srun session. From the root of the repo, run:
     $ python training_scripts/workflow_template_example.py
"""

user = getpass.getuser()
wf_uuid = uuid.uuid4()

# Create a tool
tool = Tool(name="example_tool")

# Create a workflow, and set the executor
workflow = tool.create_workflow(
    name=f"example_workflow_{wf_uuid}",
)

# Create task templates

data_prep_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 1,
        "memory": "1G",
        "runtime": "1m",
        "stdout": f"/ihme/scratch/users/{user}",
        "stderr": f"/ihme/scratch/users/{user}",
        "project": "proj_scicomp",
        "constraints": "archive"  # To request a J-drive access node, although this is only as an aexample
    },
    template_name="quickstart_data_prep_template",
    default_cluster_name="slurm",
    command_template="python"
                     "/mnt/team/scicomp/pub/docs/training_scripts/quickstart/data_prep.py "
                     "--log_level {log_level}",
    node_args=[],
    task_args=["location_set_id"],
    op_args=["log_level"],
)


parallel_by_location_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 2,
        "memory": "1G",
        "runtime": "10m",
        "stdout": f"/ihme/scratch/users/{user}",
        "stderr": f"/ihme/scratch/users/{user}",
        "project": "proj_scicomp"
    },
    template_name="quickstart_location_template",
    default_cluster_name="slurm",
    command_template="{python}"
                     "/mnt/team/scicomp/pub/docs/training_scripts/quickstart/one_location.py "
                     "--location_id {location_id} "
                     "--log_level {log_level} ",
    node_args=["location_id"],
    task_args=["python", "parallel_by_location_script_path"],
    op_args=["log_level"],
)

parallel_location_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 2,
        "memory": "1G",
        "runtime": "10m",
        "stdout": f"/ihme/scratch/users/{user}",
        "stderr": f"/ihme/scratch/users/{user}",
        "project": "proj_scicomp"
    },
    template_name="quickstart_summarization_template",
    default_cluster_name="slurm",
    command_template="{python} /mnt/team/scicomp/pub/docs/training_scripts/quickstart/summarization.py "
                     "--log_level {log_level}",
    node_args=[],
    task_args=["python", "summarization_script_path"],
    op_args=["log_level"],
)


# Create tasks
location_set_id = 5
location_set = list(range(location_set_id))

data_prep_task = data_prep_template.create_task(name="data_prep", output="data_prep")
data_prep_task = data_prep_template.create_task(
    name="data_prep_task",
    upstream_tasks=[],
    output="data_prep_task",
    location_set_id=location_set_id,
    log_level="DEBUG"
)

location_tasks = python_template.create_tasks(
    python=sys.executable,
    script_path=script_path,
    location_id=location_set
)

summarization_task = python_template.create_tasks(
    python=sys.executable,
    script_path=script_path,
    log_level="DEBUG"
)


# add task to workflow
workflow.add_tasks([data_prep_task])
workflow.add_tasks(location_tasks)
workflow.add_tasks([summarization_task])

# Connect the dependencies. Notice the use of get_tasks_by_node_args

for loc_id in location_set:
    single_task = workflow.get_tasks_by_node_args("quickstart_location_template", {"location_id": loc_id})
    single_task[0].add_upstream(data_prep_task)
    summarization_task.add_upstream(single_task)


# Calling workflow.bind() first just so that we can get the workflow id
workflow_id = workflow.bind()
print("Workflow creation complete, running workflow. For full information see the Jobmon GUI:")
print(f"https://jobmon-gui.ihme.washington.edu/#/workflow/{workflow.workflow_id}/tasks")


# run workflow
status = workflow.run()
print(f"Workflow {workflow.workflow_id} completed with status {status}.")