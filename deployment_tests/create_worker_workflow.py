import getpass
import os
import sys

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask

from jobmon.models.workflow_run_status import WorkflowRunStatus

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def create_worker_workflow(num, wfid) -> None:
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    user = getpass.getuser()
    worker_wf = Workflow(f"worker_wf_{num}_{wfid}",
                         name=f"worker_workflow_{num}_{wfid}",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{num}",
                         project="proj_scicomp",
                         resume=True)
    task_1 = BashTask(f"{command} 10", num_cores=1)
    task_2 = BashTask(f"{command} 15", num_cores=1)
    task_3 = BashTask(f"{command} 20", num_cores=1)

    worker_wf.add_tasks([task_1, task_2, task_3])
    wfr = worker_wf.run()
    if wfr.status != WorkflowRunStatus.DONE:
        raise ValueError(f"workflow run: {wfr.workflow_run_id} did not finish successfully. "
                         f"status is {wfr.status}")


if __name__ == "__main__":
    num = sys.argv[1]
    wfid = sys.argv[2]

    create_worker_workflow(num, wfid)
