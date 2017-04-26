import os
import time
import pytest

from jobmon.models import Status
from jobmon.job import Job

try:
    from jobmon.executors.sge_exec import SGEExecutor
except KeyError:
    pass

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_sge_executor(central_jobmon):

    runfile = os.path.join(here, "waiter.py")
    j1 = Job(
        central_jobmon.out_dir,
        name="job1",
        runfile=runfile,
        job_args=["30"])
    j2 = Job(
        central_jobmon.out_dir,
        name="job2",
        runfile=runfile,
        job_args=["30"])
    j3 = Job(
        central_jobmon.out_dir,
        name="job3",
        runfile=runfile,
        job_args=["30"])

    sgexec = SGEExecutor(
        central_jobmon.out_dir, 3, 30000, parallelism=2)
    sgexec.queue_job(j1)
    sgexec.queue_job(j2)
    sgexec.queue_job(j3)

    sgexec.refresh_queues(flush_lost_jobs=False)
    assert len(sgexec.running_jobs) == 2
    assert len(sgexec.queued_jobs) == 1

    while len(sgexec.queued_jobs) > 0 or len(sgexec.running_jobs) > 0:
        time.sleep(60)
        sgexec.refresh_queues(flush_lost_jobs=True)

    assert (
        [j.name for j in
         central_jobmon.jobs_with_status(Status.COMPLETE)] == [
            "job1", "job2", "job3"])
