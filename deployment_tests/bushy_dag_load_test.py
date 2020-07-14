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


def bushy_dag_load_test(n_jobs: int) -> None:
    """
    Creates and runs one workflow with n jobs and another 3n jobs that have
    dependencies to the previous n jobsm then a final tier that has n jobs
    with multiple dependencies on the 3n job tier. This can test how jobmon
    will respond when there is a large load of connections and communication
    to and from the db (like when dismod or codcorrect run)
    """
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"load-test_{wfid}", "bushy_dag_test",
                  stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  project="proj_scicomp")

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(f"echo {uid}", num_cores=1)
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(f"echo {uid}",
                               upstream_tasks=[tier1[(i % n_jobs)]], num_cores=1)
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)

    num_tasks = 5 * n_jobs
    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} tasks in "
          f"this DAG")
    wf.run()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


if __name__ == "__main__":
    n_jobs = 100
    if len(sys.argv) > 1:
        n_jobs = int(sys.argv[1])
        assert n_jobs > 0, "Please provide an integer greater than 0 for the" \
                           " number of jobs"
    bushy_dag_load_test(n_jobs)

