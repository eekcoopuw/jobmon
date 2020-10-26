import getpass
import os
import stat
import sys
import uuid

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.python_task import PythonTask


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def multi_workflow_test(n_workflows: int, n_tasks: int) -> None:
    """
    Creates and runs a workflow that creates and runs n workflows that each
    run 3 sleeping jobs. This will test the capacity for jobmon services to
    handle the requests that come from a large number of reconciler and job
    instance factory instances

    ex command to replicate codem behavior: python multi_workflow_test.py 700
    """

    wfid = uuid.uuid4()
    # wfid = "49ac544c-38a4-4c9c-aad6-d6e574c8fceb"
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
                         project="proj_scicomp")
    for i in range(n_workflows):
        tmp_hash = uuid.uuid4()
        task = PythonTask(script=f"{script}", args=[i, tmp_hash, n_tasks],
                          name=f"worker_{i}", num_cores=2, m_mem_free='1G')
        master_wf.add_task(task)
    status = master_wf.run()

    return status


if __name__ == "__main__":

    try:
        n_wfs = int(sys.argv[1])
        n_tasks = int(sys.argv[2])
    except IndexError:
        n_wfs = 500
        n_tasks = 100

    result = multi_workflow_test(n_wfs, n_tasks)
    if result.status != 'D':
        raise RuntimeError("Workflow failed")