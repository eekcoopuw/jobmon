from argparse import ArgumentParser, Namespace
from datetime import datetime
import getpass
import os
import random
import shlex
import stat
from typing import Optional
import uuid


from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
random.seed(12345)


def three_phase_load_test(n_jobs: int, wfid: str = "") -> None:
    """
    Creates and runs one workflow with n jobs and another 3n jobs that have
    dependencies to the previous n jobsm then a final tier that has n jobs
    with multiple dependencies on the 3n job tier. This can test how jobmon
    will respond when there is a large load of connections and communication
    to and from the db (like when dismod or codcorrect run)
    """
    if not wfid:
        wfid = str(uuid.uuid4())
    user = getpass.getuser()
    wf = Workflow(f"load-test_{wfid}", "load_test",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_scicomp")

    command = os.path.join(thisdir, "sleep_and_echo.sh")
    st = os.stat(command)
    os.chmod(command, st.st_mode | stat.S_IXUSR)

    tier1 = []
    counter = 0
    # First Tier
    for i in range(n_jobs):
        counter += 1
        sleep_time = random.randint(30, 41)
        # 3  new attributes per task with values to cause full new bind for every attribute
        attributes = {f'foo_{i}': str(sleep_time), f'bar_{i}': str(sleep_time),
                      f'baz_{i}': str(sleep_time)}
        tier_1_task = BashTask(f"{command} {sleep_time} {counter} {wfid}",
                               task_attributes=attributes, num_cores=1)
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs * 3):
        counter += 1
        sleep_time = random.randint(30, 41)
        # Same 3 attributes for every tier 2 task
        attributes = {'foo': 'foo_val', 'bar': 'bar_val', 'baz': 'baz_val'}
        tier_2_task = BashTask(f"{command} {sleep_time} {counter} {wfid}",
                               task_attributes=attributes,
                               upstream_tasks=[tier1[(i % n_jobs)]], num_cores=1)
        tier2.append(tier_2_task)

    tier3 = []
    # Third Tier, depend on 3 tier 2 tasks
    for i in range(n_jobs):
        counter += 1
        sleep_time = random.randint(30, 41)
        # each task has 30-40 attributes with no value assigned yet
        attributes = []
        for j in range(sleep_time):
            attributes.append(f'attr_{j}')
        tier_3_task = BashTask(f"{command} {sleep_time} {counter} {wfid}",
                               task_attributes=attributes,
                               upstream_tasks=[tier2[i], tier2[(i + n_jobs)],
                                               tier2[(i + (2 * n_jobs))]],
                               num_cores=1)
        tier3.append(tier_3_task)

    wf.add_tasks(tier1 + tier2 + tier3)

    num_tasks = 5 * n_jobs
    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} tasks in "
          f"this DAG")
    wf.run()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


def parse_arguments(argstr: Optional[str] = None) -> Namespace:
    """
    Gets arguments from the command line or a command line string.
    """
    parser = ArgumentParser()
    parser.add_argument("--num", type=int, required=True, default=1)
    parser.add_argument("--wfid", type=str, required=True)

    if argstr is not None:
        arglist = shlex.split(argstr)
        args = parser.parse_args(arglist)
    else:
        args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_arguments()
    three_phase_load_test(n_jobs=args.num, wfid=args.wfid)
