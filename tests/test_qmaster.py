import os
import pytest

from jobmon import qmaster
from jobmon.schedulers import RetryScheduler

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_job_queue(central_jobmon):

    q = qmaster.JobQueue(
        central_jobmon.out_dir, executor_params={"parallelism": 20})

    runfile = os.path.join(here, "waiter.py")
    for name in ["_" + str(num) for num in range(1, 51)]:
        j = q.create_job(
            jobname=name,
            runfile=runfile,
            parameters=[1])
        q.queue_job(j)

    q.block_till_done(poll_interval=7)

    assert len(q.executor.completed_jobs) == 50
    q.executor.stop()


@pytest.mark.cluster
def test_retry_queue(central_jobmon):

    q = qmaster.JobQueue(
        central_jobmon.out_dir, scheduler=RetryScheduler)

    runfile = os.path.join(here, "failer.py")
    for name in ["_" + str(num) for num in range(1, 6)]:
        j = q.create_job(
            jobname=name,
            runfile=runfile,
            parameters=["raise"])
        q.queue_job(j)

    runfile = os.path.join(here, "waiter.py")
    for name in ["_" + str(num) for num in range(1, 6)]:
        j = q.create_job(
            jobname=name,
            runfile=runfile,
            parameters=[5])
        q.queue_job(j)

    q.block_till_done(poll_interval=7)
    assert len(q.executor.completed_jobs) == 5
    assert len(q.executor.failed_jobs) == 5
    for jid in q.executor.failed_jobs:
        assert len(q.executor.jobs[jid]["job"].job_instance_ids) == 2
    q.executor.stop()
