import os
import pytest

from jobmon import qmaster
from jobmon.connection_config import ConnectionConfig
from jobmon.schedulers import RetryScheduler

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_job_queue(central_jobmon):
    monitor_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="monitor_info.json")
    publisher_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="publisher_info.json")
    q = qmaster.JobQueue(
        monitor_connection=monitor_connection,
        publisher_connection=publisher_connection,
        executor_params={"parallelism": 20})

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
    monitor_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="monitor_info.json")
    publisher_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="publisher_info.json")

    q = qmaster.JobQueue(
        monitor_connection=monitor_connection,
        publisher_connection=publisher_connection,
        scheduler=RetryScheduler)

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
