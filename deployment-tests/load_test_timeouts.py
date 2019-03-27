import getpass
import random
import sys
import uuid
from datetime import datetime
from typing import List


from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.executors import sge_utils as sge
from mock_sleep_and_write_task import SleepAndWriteFileMockTask


def load_test_with_timeouts(n_jobs: int, n_timeouts: int, all_phases: bool)->\
        None:
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"load-test-{wfid}", "load_test_with_timeouts",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_burdenator")

    tier1 = []
    for i in range(n_jobs):
        sleep = random.randint(20, 31)
        uid = f"tier1_{uuid.uuid4()}"
        cs = sge.true_path(f"deployment-tests/sleep_and_err.py --unique_id "
                           f"{uid}")
        task = SleepAndWriteFileMockTask(
            command=f"python {cs} --sleep_secs {sleep}",
            max_runtime_seconds=35)
        tier1.append(task)

    num_tasks = n_jobs

    if all_phases:
        num_tasks = 5 * n_jobs
        tier2 = []
        # Second Tier, depend on 1 tier 1 task
        for i in range(n_jobs * 3):
            sleep = random.randint(20, 31)
            uid = f"tier2_{uuid.uuid4()}"
            cs = sge.true_path(f"deployment-tests/sleep_and_err.py --unique_id"
                               f" {uid}")
            task = SleepAndWriteFileMockTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier1[(i % n_jobs)]], max_runtime_seconds=35)
            tier2.append(task)
        # if you run all tasks all first tier tasks will run smoothly, then
        # tier 2 tasks will start, and some will error out which will cause
        # some tier 3 tasks to time out
        add_random_timeouts(tier2, (n_timeouts * 3))

        tier3 = []
        # Third Tier, depend on 3 tier 2 tasks
        for i in range(n_jobs):
            sleep = random.randint(20, 31)
            uid = f"tier3_{uuid.uuid4()}"
            cs = sge.true_path(f"deployment-tests/sleep_and_err.py --unique_id"
                               f" {uid}")
            task = SleepAndWriteFileMockTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier2[i], tier2[(i + n_jobs)],
                                tier2[(i + (2 * n_jobs))]],
                max_runtime_seconds=35)
            tier3.append(task)

        add_random_timeouts(tier3, n_timeouts)
        wf.add_tasks(tier1 + tier2 + tier3)

    else:
        add_random_timeouts(tier1, n_timeouts)
        wf.add_tasks(tier1)

    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} tasks in "
          f"this DAG")
    wf.execute()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


def add_random_timeouts(task_list: List, n_timeouts: int) -> None:
    """set a random group of tasks to timeout. Seeded with 4 so we can
    replicate the random group """
    random.seed(4)
    sample = random.sample(task_list, n_timeouts)
    for task in sample:
        task.fail_count = 2


if __name__ == "__main__":
    """
    ex. call ' python deployment-tests/load_test_with_exceptions.py 1 0 
    --all_phases'
     to run three tiers 1 job, 3 jobs, 1 job, without any raising exceptions
    """
    n_jobs = 1
    n_timeouts = 0  # default set to not have any jobs that time out
    all_phases = False
    if len(sys.argv) > 1:
        n_jobs = int(sys.argv[1])
        n_timeouts = int(sys.argv[2])
        if len(sys.argv) > 3:
            if sys.argv[3] == '--all_phases':
                all_phases = True
        assert n_jobs > 0, "Please provide an integer greater than 0 for the" \
                           f" number of jobs: {n_jobs}"
        assert (0 <= n_timeouts <= n_jobs), "Please provide a value for the"\
                                     " number of jobs that will error"\
                                     " out that is less than or equal to the " \
                                     "number of jobs and not negative"
    load_test_with_timeouts(n_jobs, n_timeouts, all_phases=all_phases)