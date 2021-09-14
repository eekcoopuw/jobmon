import uuid

from jobmon.client.api import Tool


def six_job_test():
    """
    Creates and runs one workflow with six jobs. Used to 'smoke test' a
    new deployment of jobmon.
    """
    # First Tier
    # Deliberately put in on the long queue with max runtime > 1 day
    tool = Tool(name="Iguanodon alpha testing - Slurm")

    tt = tool.get_task_template(
        template_name='alpha_testing_template',
        command_template="{command}",
        node_args=['command'],
        default_cluster_name='slurm',
        default_compute_resources={'memory': 1,
                                   'cores': 1,
                                   'runtime': 60,
                                   'queue': 'all.q',
                                   'project': 'general',
                                   'stderr': "./six_job_test_stderr.log",
                                   'stdout': "./six_job_test_stdout.log"}
    )

    t1 = tt.create_task(
        name='t1',
        command="sleep 10",
        compute_resources={
            'queue': 'long.q',
            'runtime': 90_000})

    # Second Tier, both depend on first tier
    t2 = tt.create_task(
        name='t2',
        command="sleep 20",
        upstream_tasks=[t1])

    t3 = tt.create_task(
        name='t3',
        command="sleep 25",
        upstream_tasks=[t1])

    # Third Tier, cross product dependency on second tier
    t4 = tt.create_task(
        name='t4',
        command="sleep 17",
        upstream_tasks=[t2, t3])

    t5 = tt.create_task(
        name='t5',
        command="sleep 13",
        upstream_tasks=[t2, t3])

    # Fourth Tier, ties it all back together
    t6 = tt.create_task(
        name='t6',
        command="sleep 19",
        upstream_tasks=[t4, t5])

    wf = tool.create_workflow(
        workflow_args="six-job-test-{}".format(uuid.uuid4()),
        name='alpha_testing_')
    wf.add_tasks([t1, t2, t3, t4, t5, t6])
    print("Running the workflow, about 70 seconds minimum")
    wfr_status = wf.run()
    if wfr_status == 'D':
        print("All good, dancing pixies.")
    else:
        raise ValueError(f"Workflow should be successful, not state {wfr_status}")


if __name__ == "__main__":
    six_job_test()
