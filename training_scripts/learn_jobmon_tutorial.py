from getpass import getuser
import sys
from jobmon.client.api import Tool

# Setup: Pull in location IDs, sex IDs, and username
user = getuser()
most_detailed_location_ids = [101, 102, 103, 104]
sex_ids = [1, 2]

# Create a tool and a workflow
codcorrect_tool = Tool(name="codcorrect_sample")
workflow = codcorrect_tool.create_workflow(name="codcorrect_2021", workflow_args="cc_V23")

# Set resources on the workflow
workflow.set_default_compute_resources_from_dict(
    cluster_name='slurm',
    dictionary={
        'memory': '1G',
        'cores': 2,
        'runtime': '1000s',
        'constraints': 'archive',
        'stdout': f"/ihme/scratch/users/{user}/out",
        'project': 'proj_scicomp',
        'queue': 'all.q'
    })

# Create two task templates
correct_template = codcorrect_tool.get_task_template(
    template_name="correct", command_template="{python} correct.py --country {country} --sex {sex}",
    node_args=['country', 'sex'], op_args=['python'], task_args=[])
aggregate_template = codcorrect_tool.get_task_template(
    template_name="aggregate", command_template="{python} aggregate.py --country {country}", node_args=['country'],
    op_args=['python'], task_args=[])

# Update resources for "correct" tasks, from a YAML file
correct_template.set_default_compute_resources_from_yaml(
    yaml_file="./learn_jobmon_resources.yaml", default_cluster_name='slurm')

# Create our tasks
correct_tasks = {}  # Initialize a container for the first-stage tasks
for loc in most_detailed_location_ids:
    loc_tasks = []  # Initialize a container for all location-specific tasks
    for sex in sex_ids:
        task = correct_template.create_task(name=f"correct_{loc}_{sex}",
                                            country=loc, sex=sex, python=sys.executable)
        loc_tasks.append(task)
        # Add loc-sex specific task to the workflow
        workflow.add_task(task)
    correct_tasks[loc] = loc_tasks  # Add the location-specific tasks to the dictionary
    agg_task = aggregate_template.create_task(
        name=f"aggregation_{loc}",
        # Run after the 2 upstream location-specific tasks
        upstream_tasks=correct_tasks[loc],
        country=loc, python=sys.executable)
    # Add the aggregation task to the workflow
    workflow.add_task(agg_task)

# Run the workflow
status = workflow.run()
print(f"Workflow finished with status: {status}")



