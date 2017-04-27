import os
import pytest

from jobmon import qmaster
from mock_job import MockJob


try:
    from jobmon.executors.sge_exec import SGEExecutor
except KeyError:
    pass


@pytest.mark.cluster
def test_five_jobs(central_jobmon_cluster):
    """Submit five jobs through the job monitor.
    Three run to successful completion,
    one raises an exception, and one simply kills its own python executable.

    Note that logging to console is captured by pytest, only file logs are
    available. See http://doc.pytest.org/en/latest/capture.html
    """

    root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

    q = qmaster.JobQueue(central_jobmon_cluster.out_dir, executor=SGEExecutor,
                         executor_params={"parallelism": 10})

    # They take 5, 10, 15,.. seconds to run.
    # The third job will throw and exception, and the 4th one will
    # just call os.exit
    exceptions = ['', '', "bad_job", MockJob.DIE_COMMAND, '']

    runfile = "{root}/tests/mock_job.py".format(root=root)
    for i in [i for i in range(5)]:
        j = q.create_job(
            runfile=runfile,
            jobname="mock_{}".format(i),
            parameters=["job_" + str(i), str(5 * i), exceptions[i]])
        q.queue_job(j, slots=2, memory=4, project="ihme_general")

    q.block_till_done()  # monitor them

    sge_stats = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance"]})

    sge_errors = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance_error"]})

    # Check the 3 were good, and 2 died
    assert len(q.executor.failed_jobs) == 2
    assert len(q.executor.completed_jobs) == 3

    # Check that the proper number of errors are recorded
    assert len(sge_errors[1]) == len(q.executor.failed_jobs)

    # Check that usage stats are populating
    for stats in sge_stats[1]:
        assert '00:00' in stats['cpu']
