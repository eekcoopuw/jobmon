import getpass
import os
import random
import stat
import sys
import uuid
from datetime import datetime

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


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
        tier_1_task = BashTask(f"{command} {sleep_time} {counter} {wfid}", num_cores=1)
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs * 3):
        counter += 1
        sleep_time = random.randint(30, 41)
        tier_2_task = BashTask(f"{command} {sleep_time} {counter} {wfid}",
                               upstream_tasks=[tier1[(i % n_jobs)]], num_cores=1)
        tier2.append(tier_2_task)

    tier3 = []
    # Third Tier, depend on 3 tier 2 tasks
    for i in range(n_jobs):
        counter += 1
        sleep_time = random.randint(30, 41)
        tier_3_task = BashTask(f"{command} {sleep_time} {counter} {wfid}",
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


if __name__ == "__main__":
    n_jobs = 1
    if len(sys.argv) > 1:
        n_jobs = int(sys.argv[1])
        assert n_jobs > 0, "Please provide an integer greater than 0 for the" \
                           " number of jobs"
    three_phase_load_test(n_jobs)
