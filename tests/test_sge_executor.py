import os
import time
import pytest

from jobmon.connection_config import ConnectionConfig
from jobmon.models import Status
from jobmon.job import Job

try:
    from jobmon.executors.sge_exec import SGEExecutor
except KeyError:
    pass

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_sge_executor(central_jobmon_cluster):
    monitor_connection = ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir, monitor_filename="monitor_info.json")
    publisher_connection = ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir, monitor_filename="publisher_info.json")

    runfile = os.path.join(here, "waiter.py")
    j1 = Job(
        monitor_connection,
        name="job1",
        runfile=runfile,
        job_args=["30"])
    j2 = Job(
        monitor_connection,
        name="job2",
        runfile=runfile,
        job_args=["30"])
    j3 = Job(
        monitor_connection,
        name="job3",
        runfile=runfile,
        job_args=["30"])

    sgexec = SGEExecutor(
        monitor_connection=monitor_connection, publisher_connection=publisher_connection, parallelism=2)
    sgexec.queue_job(j1)
    sgexec.queue_job(j2)
    sgexec.queue_job(j3)

    sgexec.refresh_queues(flush_lost_jobs=False)
    assert len(sgexec.running_jobs) == 2
    assert len(sgexec.queued_jobs) == 1

    while len(sgexec.queued_jobs) > 0 or len(sgexec.running_jobs) > 0:
        time.sleep(60)
        sgexec.refresh_queues(flush_lost_jobs=True)
    complete_jobs = set(
        [j.name for j in
         central_jobmon_cluster.jobs_with_status(Status.COMPLETE)])
    exp_complete_jobs = set(["job1", "job2", "job3"])
    assert complete_jobs == exp_complete_jobs
