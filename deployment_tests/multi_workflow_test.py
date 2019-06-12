import getpass
import os
import stat
import sys
import uuid

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon import PythonTask

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def multi_workflow_test(n_workflows: int) -> None:
    """
    Creates and runs a workflow that creates and runs n workflows that each
    run 3 sleeping jobs. This will test the capacity for jobmon services to
    handle the requests that come from a large number of reconciler and job
    instance factory instances

    ex command to replicate codem behavior: python multi_workflow_test.py 700
    """

    wfid = uuid.uuid4()
    user = getpass.getuser()
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    st = os.stat(command)
    os.chmod(command, st.st_mode | stat.S_IXUSR)

    script = os.path.join(thisdir, "create_worker_workflow.py")
    st = os.stat(script)
    os.chmod(script, st.st_mode | stat.S_IXUSR)

    master_wf = Workflow(f"master_workflow_{wfid}",
                         name="master_workflow",
                         stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                         stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                         project="proj_tools")
    for i in range(n_workflows):
        task = PythonTask(script=f"{script} {i} {wfid}",
                          name=f"worker_{i}", num_cores=2, m_mem_free='1G')
        master_wf.add_task(task)
    master_wf.run()


if __name__ == "__main__":
    n_wfs = 1
    if len(sys.argv) > 1:
        n_wfs = int(sys.argv[1])
        assert n_wfs > 0, "Please provide an integer greater than 0 for the " \
                          "number of workflows"
    multi_workflow_test(n_wfs)
