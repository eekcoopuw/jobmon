import getpass
import os
import random
import stat
import sys
import uuid
from datetime import datetime

from jobmon.client.swarm.workflow.workflow import Workflow

from jobmon import BashTask


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def one_phase_load_test(n_jobs: int) -> None:
    """
    Creates and runs one workflow with n jobs to test when all of the jobs
    get bound and queued at the same time
    :param n_jobs: number of jobs you want to run
    :return:
    """

    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"one-phase-load-test_{wfid}", "one_phase_load_test",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_tools")

    command = os.path.join(thisdir, "sleep_and_echo.sh")
    st = os.stat(command)
    os.chmod(command, st.st_mode | stat.S_IXUSR)

    task_list = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        sleep_time = random.randint(30, 41)
        task = BashTask(f"{command} {sleep_time} {uid}", slots=1)
        task_list.append(task)

    wf.add_tasks(task_list)
    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(
        f"{time}: Beginning the workflow, there are {n_jobs} tasks in this "
        f"DAG")
    wf.execute()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


if __name__ == "__main__":
    n_jobs = 1
    if len(sys.argv) > 1:
        n_jobs = int(sys.argv[1])
        assert n_jobs > 0, "Please provide an integer greater than 0 for " \
                           "the number of jobs"
    one_phase_load_test(n_jobs)
