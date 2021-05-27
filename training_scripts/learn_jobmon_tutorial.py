from getpass import getuser
import sys

from jobmon.client.api import ExecutorParameters, Tool

user = getuser()
most_detailed_location_ids = [101, 102, 103, 104]
sex_ids = [1, 2]
codcorrect_tool = Tool.create_tool(name="codcorrect_sample")
workflow = codcorrect_tool.create_workflow(name="codcorrect_2021", workflow_args="cc_V23")
workflow.set_executor(executor_class="SGEExecutor", project="proj_scicomp", stderr=f"/ihme/scratch/users/{user}/err",
                      stdout=f"/ihme/scratch/users/{user}/out", working_dir=f"/homes/{user}")
correct_template = codcorrect_tool.get_task_template(
    template_name="correct", command_template="{python} correct.py --country {country} --sex {sex}",
    node_args=['country', 'sex'], op_args=['python'], task_args=[])
aggregate_template = codcorrect_tool.get_task_template(
    template_name="aggregate", command_template="{python} aggregate.py --country {country}", node_args=['country'],
    op_args=['python'], task_args=[])
correct_exec_params = ExecutorParameters(executor_class="SGEExecutor", m_mem_free="36", num_cores=3, queue="all.q",
                                         max_runtime_seconds=200)
aggregate_exec_params = ExecutorParameters(executor_class="SGEExecutor", m_mem_free="6G", num_cores=3, queue="all.q",
                                           max_runtime_seconds=600)
correct_tasks = {}
for loc in most_detailed_location_ids:
    loc_tasks = []
    for sex in sex_ids:
        task = correct_template.create_task(name=f"correct_{loc}_{sex}", executor_parameters=correct_exec_params,
                                            country=loc, sex=sex, python=sys.executable)
        loc_tasks.append(task)
        workflow.add_task(task)
    correct_tasks[loc] = loc_tasks
    agg_task = aggregate_template.create_task(name=f"aggregation_{loc}", executor_parameters=aggregate_exec_params,
                                              upstream_tasks=correct_tasks[loc], country=loc, python=sys.executable)
    workflow.add_task(agg_task)

workflow_run = workflow.run()
print(f"Workflow finished with status: {workflow_run.status}")
