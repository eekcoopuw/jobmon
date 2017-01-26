import json
import os
import pytest
import subprocess

from jobmon import job
from jobmon import qmaster
from jobmon.models import Status
from jobmon.setup_logger import setup_logger
from mock_job import MockJob

# See Jira GBDSCI-114, 119
# Commented out all uses of qmaster so that this will not break the jenkins build.
# needs to be refactored to use the test fixture instance of central_job_monitor, and
# the loop style of dalynator.run_all. Probably bring in the job-wait loop from there.
@pytest.mark.cluster
def test_five_jobs(central_jobmon):
    """Submit five jobs through the job monitor.
    Three run to successful completion,
    one raises an exception, and one simply kills its own python executable.

    Note that logging to console is captured by pytest, only file logs are available.
    See http://doc.pytest.org/en/latest/capture.html

    Uses two environment variables:
    CONDA_ROOT  path to the conda executable for the central_monitor process and the remote jobs
    CONDA_ENV   name of the environment for the central_monitor process and the remote jobs
    """

    root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

    # TODO Update these hard-wired strings after decision on test environments.
    conda_info = json.loads(
        subprocess.check_output(['conda', 'info', '--json']))
    path_to_conda_bin_on_target_vm = '{}/bin'.format(conda_info['root_prefix'])
    conda_env = conda_info['default_prefix'].split("/")[-1]
    q = qmaster.MonitoredQ(central_jobmon.out_dir,
                           path_to_conda_bin_on_target_vm,
                           conda_env,
                           request_timeout=30000)

    # They take 5, 10, 15,.. seconds to run.
    # The third job will throw and exception, and the 4th one will
    # just call os.exit
    exceptions = ['', '', "bad_job", MockJob.DIE_COMMAND, '']

    for i in [i for i in range(5)]:
        mjid, sgeid = q.qsub(
            runfile="{root}/tests/mock_job.py".format(root=root),
            jobname="mock_{}".format(i),
            parameters=["job_" + str(i), 5 * i, exceptions[i]],
            slots=2,
            memory=4,
            project="ihme_general")
    q.qblock(poll_interval=30)  # monitor them

    # query returns a dataframe
    failed = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job where current_status = {}".format(
             Status.FAILED)]})

    complete = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job where current_status = {}".format(
             Status.COMPLETE)]})

    sge_stats = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from sge_job"]})

    sge_errors = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from error"]})

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
