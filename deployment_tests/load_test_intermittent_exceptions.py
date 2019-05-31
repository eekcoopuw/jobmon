import argparse
import getpass
import os
import random
import uuid
from datetime import datetime
from typing import List

from deployment_tests.mock_load_test_task import MockLoadTestTask
from deployment_tests.sleep_and_error import SLEEP_TOO_LONG
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import Workflow

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
script = os.path.join(thisdir, "sleep_and_error.py")

SAFE_RUNTIME_SECONDS = SLEEP_TOO_LONG - 10


def load_test_with_timeouts(n_jobs: int, n_exceptions: int,
                            sleep_timeout: bool, all_phases: bool) -> None:
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"load-test-{wfid}", "load_test_with_timeouts",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_tools")

    tier1 = []
    for i in range(n_jobs):
        sleep = sleep_range()
        cs = f"{script} --uid tier1_{uuid.uuid4()}"
        task = MockLoadTestTask(
            command=f"python {cs} --sleep_secs {sleep}",
            max_runtime_seconds=SAFE_RUNTIME_SECONDS)
        tier1.append(task)

    add_random_timeouts(tier1, n_exceptions, sleep_timeout)
    wf.add_tasks(tier1)
    num_tasks = n_jobs

    if all_phases:
        num_tasks = 5 * n_jobs
        tier2 = []
        # Second Tier, depend on 1 tier 1 task
        for i in range(n_jobs * 3):
            sleep = sleep_range()
            cs = f"{script} --uid tier2_{uuid.uuid4()}"
            task = MockLoadTestTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier1[(i % n_jobs)]],
                max_runtime_seconds=SAFE_RUNTIME_SECONDS)
            tier2.append(task)
        # if you run all tasks all first tier tasks will run smoothly, then
        # tier 2 tasks will start, and some will error out which will cause
        # some tier 3 tasks to time out
        add_random_timeouts(tier2, (n_exceptions * 3), sleep_timeout)

        tier3 = []
        # Third Tier, depend on 3 tier 2 tasks
        for i in range(n_jobs):
            sleep = sleep_range()
            cs = f"{script} --uid tier3_{uuid.uuid4()}"
            task = MockLoadTestTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier2[i], tier2[(i + n_jobs)],
                                tier2[(i + (2 * n_jobs))]],
                max_runtime_seconds=SAFE_RUNTIME_SECONDS)
            tier3.append(task)

        add_random_timeouts(tier3, n_exceptions, sleep_timeout)
        wf.add_tasks(tier2 + tier3)

    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} "
          "tasks in this DAG")
    status = wf.execute()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    status_string = "Succeeded" \
        if status == DagExecutionStatus.SUCCEEDED \
        else "Failed"
    print(f"{time}: Workflow {status_string}!  workflow_id is {wf.id}")


def sleep_range() -> int:
    """Returns a runtime within the safe zone, won't be killed by timeout"""
    return random.randint(5, SAFE_RUNTIME_SECONDS - 10)


def add_random_timeouts(task_list: List, n_exceptions: int,
                        sleep_timeout: bool) -> None:
    """set a random group of tasks to timeout. Seeded with 4 so we can
    replicate the random group """
    random.seed(4)
    seed_list = list(range(len(task_list)))  # need to make a reproducible list
    sample = random.sample(seed_list, n_exceptions)
    for index in sample:
        task = task_list[index]
        task.fail_count = 1
        task.command += f" --fail_count {task.fail_count} " \
                        f"--fail_count_fp /ihme/scratch/users/" \
                        f"{getpass.getuser()}/tests/load_test"
        if sleep_timeout:
            task.sleep_timeout = True
            task.command += " --sleep_timeout"


if __name__ == "__main__":
    """
    E.g. call 'python deployment_tests/load_test_intermittent_exceptions.py
    --n_jobs 2 --n_exceptions 1
    --all_phases'
     to run three tiers, 2 job, 6 jobs, 2 job, 1 exception per every 2 jobs
    """
    parser = argparse.ArgumentParser(description='load test')
    parser.add_argument('--n_jobs', type=int, default=1, action='store',
                        help='The number of jobs for the first phase (total '
                             'will be 5 times this number if you run all '
                             'values')
    parser.add_argument('--n_exceptions', type=int, default=0, action='store',
                        help='How many exceptions you want per n_jobs, these '
                             'jobs will throw an exception the first time and '
                             'succeed the second time')
    parser.add_argument('--sleep_timeout', action='store_true', default=False,
                        help='If you want sleep timeout exceptions instead of '
                             'valueError exceptions')
    parser.add_argument('--all_phases', action='store_true', default=False,
                        help='If you want to run all three phases instead of '
                             'just a one phase load test')
    args = parser.parse_args()

    assert args.n_jobs > 0, \
        f"Please provide an integer greater than 0 for the number of jobs: " \
        f"{args.n_jobs}"
    assert (0 <= args.n_exceptions <= args.n_jobs), \
        "Please provide a value for the number of jobs that will error " \
        "out that is less than or equal to the number of jobs and not " \
        "negative"
    load_test_with_timeouts(args.n_jobs, args.n_exceptions, args.sleep_timeout,
                            args.all_phases)
