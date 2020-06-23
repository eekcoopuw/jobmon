import uuid

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask


def six_job_test():
    """
    Creates and runs one workflow with six jobs. Used to 'smoke test' a
    new deployment of jobmon.
    """
    # First Tier
    # Deliberately put in on the long queue with max runtime > 1 day
    t1 = BashTask("sleep 10", num_cores=1,
                  queue='long.q',
                  max_runtime_seconds=90_000)

    # Second Tier, both depend on first tier
    t2 = BashTask("sleep 20", upstream_tasks=[t1], num_cores=1,
                  queue='all.q',
                  max_runtime_seconds=60)
    t3 = BashTask("sleep 25", upstream_tasks=[t1], num_cores=1)

    # Third Tier, cross product dependency on second tier
    t4 = BashTask("sleep 17", upstream_tasks=[t2, t3], num_cores=1)
    t5 = BashTask("sleep 13", upstream_tasks=[t2, t3], num_cores=1)

    # Fourth Tier, ties it all back together
    t6 = BashTask("sleep 19", upstream_tasks=[t4, t5], num_cores=1)

    wf = Workflow("six-job-test-{}".format(uuid.uuid4()),
                  "./six_job_test_stderr.log",
                  project="proj_scicomp")
    wf.add_tasks([t1, t2, t3, t4, t5, t6])
    print("Running the workflow, about 70 seconds minimum")
    wf.run()
    print("All good, dancing pixies.")


if __name__ == "__main__":
    six_job_test()
