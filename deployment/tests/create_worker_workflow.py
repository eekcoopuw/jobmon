import getpass
import os
import sys

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask

from jobmon.constants import WorkflowRunStatus

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def create_worker_workflow(num, wfid, num_tasks=3) -> None:
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    user = getpass.getuser()
    worker_wf = Workflow(f"worker_wf_{num}_{wfid}",
                         name=f"worker_workflow_{num}_{wfid}",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         project="proj_scicomp",
                         resume=True)

    for n in range(1, num_tasks+1):
        task = BashTask(f"{command} {n}", num_cores=1)
        worker_wf.add_task(task)

    wfr = worker_wf.run()
    if wfr.status != WorkflowRunStatus.DONE:
        raise ValueError(f"workflow run: {wfr.workflow_run_id} did not finish successfully. "
                         f"status is {wfr.status}")


if __name__ == "__main__":
    num = int(sys.argv[1])
    wfid = sys.argv[2]

    try:
        num_tasks = int(sys.argv[3])
    except IndexError:
        num_tasks = 3

    create_worker_workflow(num, wfid, num_tasks)
