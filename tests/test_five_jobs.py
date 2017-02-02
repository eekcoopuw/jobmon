import json
import os
import pytest
import subprocess


from jobmon import qmaster, executors
from jobmon.models import Status
from mock_job import MockJob


@pytest.mark.cluster
def test_five_jobs(central_jobmon):
    """Submit five jobs through the job monitor.
    Three run to successful completion,
    one raises an exception, and one simply kills its own python executable.

    Note that logging to console is captured by pytest, only file logs are
    available. See http://doc.pytest.org/en/latest/capture.html
    """

    root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

    # TODO Update these hard-wired strings after decision on test environments.
    conda_info = json.loads(
        subprocess.check_output(['conda', 'info', '--json']).decode())
    path_to_conda_bin_on_target_vm = '{}/bin'.format(conda_info['root_prefix'])
    conda_env = conda_info['default_prefix'].split("/")[-1]

    # construct executor
    sgexec = executors.SGEExecutor(
        central_jobmon.out_dir, 3, 30000, path_to_conda_bin_on_target_vm,
        conda_env,
        parallelism=10)

    q = qmaster.MonitoredQ(sgexec)

    # They take 5, 10, 15,.. seconds to run.
    # The third job will throw and exception, and the 4th one will
    # just call os.exit
    exceptions = ['', '', "bad_job", MockJob.DIE_COMMAND, '']

    runfile = "{root}/tests/mock_job.py".format(root=root)
    for i in [i for i in range(5)]:
        j = q.create_job(
            runfile=runfile,
            jobname="mock_{}".format(i),
            parameters=["job_" + str(i), 5 * i, exceptions[i]])
        q.queue_job(j, slots=2, memory=4, project="ihme_general")

    q.check_pulse()  # monitor them

    # query returns a dataframe
    failed = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance where current_status = {}".
                  format(Status.FAILED)]})

    complete = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance where current_status = {}".
                  format(Status.COMPLETE)]})

    sge_stats = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance"]})

    sge_errors = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance_error"]})

    # Check the 3 were good, and 2 died
    number_failed = len(failed[1])
    assert number_failed == 2
    number_complete = len(complete[1])
    assert number_complete == 3

    # Check that the proper number of errors are recorded
    assert len(sge_errors[1]) == number_failed

    # Check that usage stats are populating
    for stats in sge_stats[1]:
        assert '00:00' in stats['cpu']
