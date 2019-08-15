import getpass
import os
import sys
import uuid

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon import BashTask

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def create_worker_workflow(num, wfid) -> None:
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    user = getpass.getuser()
    uid = uuid.uuid4()
    worker_wf = Workflow(f"worker_wf_{wfid}_{num}",
                         name=f"worker_workflow_{num}",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         project="proj_tools")
    task_1 = BashTask(f"{command} 10 {uid}", slots=1)
    task_2 = BashTask(f"{command} 15 {uid}", slots=1)
    task_3 = BashTask(f"{command} 20 {uid}", slots=1)

    worker_wf.add_tasks([task_1, task_2, task_3])
    worker_wf.run()


if __name__ == "__main__":
    num = sys.argv[1]
    wfid = sys.argv[2]

    create_worker_workflow(num, wfid)