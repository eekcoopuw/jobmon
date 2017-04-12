import time
import os
from jobmon.models import Status
from jobmon.executors import local_exec
from jobmon.job import Job

here = os.path.dirname(os.path.abspath(__file__))


def test_local_executor(central_jobmon):

    exlocal = local_exec.LocalExecutor(
        central_jobmon.out_dir, 3, 3000,
        parallelism=2)

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
        job_args=[10])
    j3 = Job(
        central_jobmon.out_dir,
        name="job3",
        runfile=runfile,
        job_args=[20])

    exlocal.queue_job(j1, process_timeout=60)
    exlocal.queue_job(j2, process_timeout=60)
    exlocal.queue_job(j3, process_timeout=60)

    exlocal.refresh_queues()
    assert len(exlocal.running_jobs) == 2
    assert len(exlocal.queued_jobs) == 1

    while len(exlocal.queued_jobs) > 0 or len(exlocal.running_jobs) > 0:
        time.sleep(20)
        exlocal.refresh_queues()

    assert (
        set([j.name for j in
             central_jobmon.jobs_with_status(Status.COMPLETE)]) == set(
            ["job1", "job2", "job3"]))

    exlocal.end()
