import time
import os
import pytest

from jobmon.connection_config import ConnectionConfig
from jobmon.models import Status
from jobmon.executors import local_exec
from jobmon.job import Job
from jobmon.requester import Requester

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_local_executor(central_jobmon):
    monitor_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="monitor_info.json")
    publisher_connection = ConnectionConfig(monitor_dir=central_jobmon.out_dir, monitor_filename="publisher_info.json")

    exlocal = local_exec.LocalExecutor(
        monitor_connection=monitor_connection,
        publisher_connection=publisher_connection,
        parallelism=2)

    runfile = os.path.join(here, "waiter.py")
    j1 = Job(
        monitor_connection=monitor_connection,
        name="job1",
        runfile=runfile,
        job_args=[30])
    j2 = Job(
        monitor_connection=monitor_connection,
        name="job2",
        runfile=runfile,
        job_args=[10])
    j3 = Job(
        monitor_connection=monitor_connection,
        name="job3",
        runfile=runfile,
        job_args=[20])

    exlocal.queue_job(j1, process_timeout=60)
    exlocal.queue_job(j2, process_timeout=60)
    exlocal.queue_job(j3, process_timeout=60)

    exlocal.refresh_queues(flush_lost_jobs=False)
    assert len(exlocal.running_jobs) == 2
    assert len(exlocal.queued_jobs) == 1

    while len(exlocal.queued_jobs) > 0 or len(exlocal.running_jobs) > 0:
        time.sleep(20)
        exlocal.refresh_queues(flush_lost_jobs=True)

    assert (
        set([j.name for j in
             central_jobmon.jobs_with_status(Status.COMPLETE)]) == set(
            ["job1", "job2", "job3"]))

    exlocal.stop()


@pytest.mark.xfail(reason="This is going to fail until logic around pub/sub "
                   "model and central vs. distributed monitoring (e.g. "
                   "host/port vs monitor_dir) gets resolved")
def test_local_executor_static(central_jobmon_static_port):

    monitor_connection_3459 = ConnectionConfig(monitor_host='localhost', monitor_port=3459)
    publisher_connection = ConnectionConfig(monitor_host='localhost', monitor_port=6666)

    exlocal = local_exec.LocalExecutor(
        monitor_connection=monitor_connection_3459,
        publisher_connection=publisher_connection,
        parallelism=2,
        subscribe_to_job_state=False)

    runfile = os.path.join(here, "waiter.py")
    j1 = Job(
        monitor_connection_3459,
        name="job1",
        runfile=runfile,
        job_args=[30])
    j2 = Job(
        monitor_connection_3459,
        name="job2",
        runfile=runfile,
        job_args=[10])
    j3 = Job(
        monitor_connection_3459,
        name="job3",
        runfile=runfile,
        job_args=[20])

    exlocal.queue_job(j1, subprocess_timeout=60)
    exlocal.queue_job(j2, subprocess_timeout=60)
    exlocal.queue_job(j3, subprocess_timeout=60)

    exlocal.refresh_queues()
    assert len(exlocal.running_jobs) == 2
    assert len(exlocal.queued_jobs) == 1

    while len(exlocal.queued_jobs) > 0 or len(exlocal.running_jobs) > 0:
        time.sleep(20)
        exlocal.refresh_queues()

    req = Requester(monitor_connection_3459)
    assert (
        set(req.send_request({'action': 'get_jobs_with_status',
                              'args': [Status.COMPLETE]})[1]) == set(
                                  [j1.jid, j2.jid, j3.jid]))
    exlocal.stop()
