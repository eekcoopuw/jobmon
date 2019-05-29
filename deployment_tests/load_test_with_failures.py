import getpass
import os
import random
import sys
import uuid
from datetime import datetime


from jobmon.client.swarm.workflow.workflow import Workflow
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
script = os.path.join(thisdir, "sleep_and_err.py")


def load_test_with_exceptions(n_jobs: int, n_ex: int, all_phases: bool=False)\
        -> None:
    """
    :param n_jobs: number of jobs to be created (*5 if running all phases)
    :param n_ex: number of exceptions per round of jobs
    (*5 if running all phases)
    :param all_phases: if you want to test a full three phase workflow or
    just one phase to test when all jobs are instantiated at once
    """
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"load-test_{wfid}", "load_test_with_exceptions",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_tools")

    tier1 = []
    for i in range(n_jobs):
        sleep = random.randint(30, 41)
        cs = f"{script} --uid tier1_{uuid.uuid4()}"
        task = SleepAndWriteFileMockTask(
            command=f"python {cs} --sleep_secs {sleep}")
        tier1.append(task)

    num_tasks = n_jobs

    if all_phases:
        num_tasks = 5 * n_jobs
        tier2 = []
        # Second Tier, depend on 1 tier 1 task
        for i in range(n_jobs * 3):
            sleep = random.randint(30, 41)
            cs = f"{script} --uid tier2_{uuid.uuid4()}"
            task = SleepAndWriteFileMockTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier1[(i % n_jobs)]])
            tier2.append(task)
        # if you run all tasks all first tier tasks will run smoothly, then
        # tier 2 tasks will start, and some will error out which will cause
        # some tier 3 tasks to time out
        add_random_err(tier2, (n_ex * 3))

        tier3 = []
        # Third Tier, depend on 3 tier 2 tasks
        for i in range(n_jobs):
            sleep = random.randint(30, 41)
            cs = f"{script} --uid tier3_{uuid.uuid4()}"
            task = SleepAndWriteFileMockTask(
                command=f"python {cs} --sleep_secs {sleep}",
                upstream_tasks=[tier2[i], tier2[(i + n_jobs)],
                                tier2[(i + (2 * n_jobs))]])
            tier3.append(task)

        add_random_err(tier3, n_ex)
        wf.add_tasks(tier1 + tier2 + tier3)

    else:
        add_random_err(tier1, n_ex)
        wf.add_tasks(tier1)

    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} tasks in "
          f"this DAG")
    wf.execute()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


def add_random_err(task_list, n_ex):
    """set a random group of tasks to fail seeded with 4 so we can replicate
    the random group """
    random.seed(4)
    sample = random.sample(task_list, n_ex)
    for task in sample:
        task.command += " --fail_always"


if __name__ == "__main__":
    """
    ex. call ' python deployment_tests/load_test_with_failures.py 1 0
    --all_phases'
     to run three tiers 1 job, 3 jobs, 1 job, without any raising exceptions
    """
    n_jobs = 1
    n_ex = 0  # default set to not have any jobs that error out
    all_phases = False
    print(len(sys.argv))
    if len(sys.argv) > 1:
        n_jobs = int(sys.argv[1])
        n_ex = int(sys.argv[2])
        if len(sys.argv) > 3:
            if sys.argv[3] == '--all_phases':
                all_phases = True
        assert n_jobs > 0, "Please provide an integer greater than 0 for the" \
                           f" number of jobs, you requested {n_jobs}"
        assert (0 <= n_ex < n_jobs), "Please provide a value for the"\
                                     " number of jobs that will error"\
                                     " out that is less than the " \
                                     " number of jobs and not negative, you " \
                                     f"requested {n_ex}"
    load_test_with_exceptions(n_jobs, n_ex, all_phases=all_phases)
