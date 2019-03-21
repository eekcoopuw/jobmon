import random
import sys
import uuid

from jobmon.client.swarm.workflow.workflow import Workflow

from jobmon import BashTask


def three_phase_load_test(n_jobs: int):
    """
    Creates and runs one workflow n jobs and another 3n jobs that have
    dependencies to the previous n jobs. This can be used to test how jobmon
    will respond when there is a large load of connections and communication
    to and from the db (like when dismod or codcorrect run)
    """
    wf = Workflow("load-test",
                  "./load_test_stderr.log",
                  project="proj_burdenator")

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = uuid.uuid4()
        sleep_time = random.randint(30, 41)
        tier_1_task = BashTask(f"echo 'tier 1 {uid}' sleep {sleep_time}", slots=1)
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs*3):
        uid = uuid.uuid4()
        sleep_time = random.randint(30, 41)
        tier_2_task = BashTask(f"echo 'tier 2 {uid}' sleep {sleep_time}",
                               upstream_tasks=[tier1[(i % n_jobs)]], slots=1)
        tier2.append(tier_2_task)


    tier3 = []
    # Third Tier, depend on 3 tier 2 tasks
    for i in range(n_jobs):
        uid = uuid.uuid4()
        sleep_time = random.randint(30, 41)
        tier_3_task = BashTask(f"echo 'tier 3 {uid}' sleep {sleep_time}",
                               upstream_tasks=[tier2[i], tier2[(i+n_jobs)-1],
                                               tier2[(i+(2*n_jobs)-1)]], slots=1)
        tier3.append(tier_3_task)

    wf.add_tasks(tier1+tier2+tier3)

    num_tasks = 5*n_jobs
    print(f"Beginning the workflow, there are {num_tasks} tasks in this DAG")
    wf.execute()
    print("Workflow complete!")


if __name__ == "__main__":
    n_jobs=4
    if len(sys.argv) > 1:
        print(sys.argv)
        n_jobs = sys.argv[1]
    three_phase_load_test(n_jobs)
