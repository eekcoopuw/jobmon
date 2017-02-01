import subprocess
import json
import os
import time
import pytest

from jobmon.models import Status
from jobmon.job import Job
from jobmon.executors import SGEExecutor

here = os.path.dirname(os.path.abspath(__file__))
sge_log = "/homes/mlsandar/temp"


@pytest.mark.cluster
def test_sge_executor(central_jobmon):

    conda_info = json.loads(
        subprocess.check_output(['conda', 'info', '--json']).decode())
    path_to_conda_bin_on_target_vm = '{}/bin'.format(conda_info['root_prefix'])
    conda_env = conda_info['default_prefix'].split("/")[-1]

    runfile = os.path.join(here, "waiter.py")
    j1 = Job(
        central_jobmon.out_dir,
        name="job1",
        runfile=runfile,
        job_args=[30])
    j2 = Job(
        central_jobmon.out_dir,
        name="job2",
        runfile=runfile,
        job_args=[30])
    j3 = Job(
        central_jobmon.out_dir,
        name="job3",
        runfile=runfile,
        job_args=[30])

    sgexec = SGEExecutor(
        central_jobmon.out_dir, 3, 30000, path_to_conda_bin_on_target_vm,
        conda_env,
        parallelism=2)
    sgexec.queue_job(j1, stderr=sge_log, stdout=sge_log)
    sgexec.queue_job(j2, stderr=sge_log, stdout=sge_log)
    sgexec.queue_job(j3, stderr=sge_log, stdout=sge_log)

    sgexec.heartbeat()
    assert len(sgexec.running_jobs) == 2
    assert len(sgexec.queued_jobs) == 1

    while len(sgexec.queued_jobs) > 0 or len(sgexec.running_jobs) > 0:
        time.sleep(60)
        sgexec.heartbeat()

    assert (
        [j.name for j in
         central_jobmon.jobs_with_status(Status.COMPLETE)] == [
            "job1", "job2", "job3"])
