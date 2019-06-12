import getpass
import os
import random
import stat
import sys
import uuid
from datetime import datetime

from jobmon.client.swarm.workflow.workflow import Workflow

from jobmon import BashTask

from jobmon import PythonTask

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def multi_workflow_test(n_workflows: int) -> None:
    """
    Creates and runs a workflow that then creates and runs n workflows
    that each run 3 sleeping jobs. This will test the capacity for jobmon to
    handle the requests that come from a large number of reconciler and job
    instance factory instances
    """

    wfid = uuid.uuid4()
    user = getpass.getuser()
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    st = os.stat(command)
    os.chmod(command, st.st_mode | stat.S_IXUSR)

    master_wf = Workflow(f"master_workflow_{wfid}",
                         name="master_workflow",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                         project="proj_tools")
    for i in n_workflows:
        task = PythonTask(script=f"create_worker_workflow({wfid}, {user}, "
                          f"{command}", name=f"worker_{i}", num_cores=2,
                          m_mem_free='1G')
        master_wf.add_task(task)
    master_wf.run()


def create_worker_workflow(wfid, user, command) -> None:
    uid = uuid.uuid4()
    worker_wf = Workflow(f"worker_wf_{wfid}_{i}",
                         name=f"worker_workflow_{i}",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{i}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}_{i}",
                         project="proj_tools")
    task_1 = BashTask(f"{command} 10 {uid}", slots=1)
    task_2 = BashTask(f"{command} 15 {uid}", slots=1)
    task_3 = BashTask(f"{command} 20 {uid}", slots=1)

    worker_wf.add_tasks([task_1, task_2, task_3])
    worker_wf.run()


if __name__ == "__main__":
    n_wfs = 1
    if len(sys.argv) > 1:
        n_wfs = int(sys.argv[1])
        assert n_wfs > 0, "Please provide an integer greater than 0 for the " \
                          "number of workflows"
    multi_workflow_test(n_wfs)