import os
import sys
import uuid

from jobmon.client.api import Tool


thisdir = os.path.dirname(os.path.realpath(__file__))


def six_job_test(cluster_name: str):
    """
    Creates and runs one workflow with six jobs. Used to 'smoke test' a
    new deployment of jobmon.
    """
    # First Tier
    # Deliberately put in on the long queue with max runtime > 1 day
    tool = Tool(name="Iguanodon alpha testing - Slurm")
    long_q_template = tool.get_task_template(
        template_name='long_queue_test',
        command_template="{command}",
        node_args=['command']
    )
    normal_q_template = tool.get_task_template(
        template_name='normal_queue_test',
        command_template="{command}",
        node_args=['command']
    )
    tool.set_default_compute_resources_from_yaml(
        default_cluster_name=cluster_name,
        yaml_file=os.path.join(thisdir, "six_job_test_resouces.yaml"),
        set_task_templates=True,
        ignore_missing_keys=True
    )

    t1 = long_q_template.create_task(
        name='t1',
        command="sleep 10"
    )

    # Second Tier, both depend on first tier
    t2 = normal_q_template.create_task(
        name='t2',
        command="sleep 20",
        upstream_tasks=[t1]
    )

    t3 = normal_q_template.create_task(
        name='t3',
        command="sleep 25",
        upstream_tasks=[t1]
    )

    # Third Tier, cross product dependency on second tier
    t4 = normal_q_template.create_task(
        name='t4',
        command="sleep 17",
        upstream_tasks=[t2, t3]
    )

    t5 = normal_q_template.create_task(
        name='t5',
        command="sleep 13",
        upstream_tasks=[t2, t3]
    )

    # Fourth Tier, ties it all back together
    t6 = normal_q_template.create_task(
        name='t6',
        command="sleep 19",
        upstream_tasks=[t4, t5]
    )

    wf = tool.create_workflow(
        workflow_args="six-job-test-{}".format(uuid.uuid4()),
        name='six_job_test')
    wf.add_tasks([t1, t2, t3, t4, t5, t6])
    print("Running the workflow, about 70 seconds minimum")
    wfr_status = wf.run()
    if wfr_status == 'D':
        print("All good, dancing pixies.")
    else:
        raise ValueError(f"Workflow should be successful, not state {wfr_status}")


if __name__ == "__main__":
    cluster_name = sys.argv[1]
    six_job_test(cluster_name)
