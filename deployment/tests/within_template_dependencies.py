import sys

from jobmon.client.tool import Tool

def create_workflow(workflow_args=''):

    # Create a 2 phase workflow with within-phase dependencies.

    t = Tool("inter_template_testing_tool")
    t.set_default_compute_resources_from_dict(
        'slurm',
        {
            'memory': '1M',
            'cores': 1,
            'runtime': '60s',
            'stdout': '/ihme/scratch/users/dhs2018/stdout',
            'stderr': '/ihme/scratch/users/dhs2018/stderr',
            'queue': 'all.q'
        }
    )
    wf = t.create_workflow(
        name='inter-template dependency wf',
        workflow_args=workflow_args
    )


    sleep_template = t.get_task_template(
        template_name='sleep_template',
        command_template="sleep {arg}",
        node_args=["arg"]
    )

    # Create a 1 second sleep task
    sleep1 = sleep_template.create_task(
        name='sleep1',
        arg='1'
    )
    phase1_tasks = [sleep1]

    # Create a new set of tasks 2-5 that depend on the first task
    phase2_tasks = [
        sleep_template.create_task(
            name=f'sleep_{i}',
            arg=str(i),
            upstream_tasks=phase1_tasks
        )
        for i in range(2,6)
    ]

    # Create a final set of tasks with fine-grained dependencies
    phase3_tasks = [
        sleep_template.create_task(
            name=f"sleep_{i}",
            arg=str(i),
            upstream_tasks=[phase2_tasks[i % 4]]
        )
        for i in range(6, 50)
    ]

    wf.add_tasks(phase1_tasks + phase2_tasks + phase3_tasks)
    wfr_status = wf.run()

    # Expectation: We should see 1 batch submitted each for phase 1 and phase 2.
    # Phase 3 should see 4 batches submitted.

    # Only 1 array object will be registered

    # Run the workflow, check that it finishes, and check the set of arrays and task instances in the database.

    if wfr_status != "D":
        raise ValueError(f"Workflow {wf.workflow_id} logged status {wfr_status}")


if __name__ == '__main__':

    try:
        wf_args = sys.argv[1]
    except IndexError:
        wf_args = ''

    # Test 1: Check that the workflow finishes, expected number of batches submitted
    # Test 2: Kill the workflow in phase 2, then resume it. Check that resume behaves as expected
    create_workflow(wf_args)
