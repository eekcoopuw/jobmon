import logging
import os

from jobmon import qmaster
from jobmon.setup_logger import setup_logger
from .mock_job import MockJob


def test_five_jobs():
    """Submit five jobs through the job monitor.
    Three run to successful completion,
    one raises an exception, and thone simply kills its own python executable.

    Uses two environment variables:
    CONDA_ROOT  path to the conda executable for the central_monitor process and the remote jobs
    CONDA_ENV   name of the environment for the central_monitor process and the remote jobs
    """

    # Start the root logger, not just the logger for my name
    logger = logging.getLogger("jobmon")
    setup_logger(logger.name, "./TestFiveJobs.log", logging.DEBUG)
    logger.info('{}: test_five_jobs started'.format(os.getpid()))

    root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

    # TODO Update these hard-wired strings after decision on test environments.
    path_to_conda_bin_on_target_vm = os.getenv('CONDA_ROOT',
                                               '/homes/gphipps/miniconda2/envs/gphipps-jobmon-1/bin/conda')
    conda_env = os.getenv('CONDA_ENV', 'gphipps-jobmon-1')
    logger.info('   CONDA_ROOT='.format(path_to_conda_bin_on_target_vm))
    logger.info('   conda-env'.format(conda_env))

    q = qmaster.MonitoredQ(".", path_to_conda_bin_on_target_vm,
                           conda_env, request_timeout=30000)  # monitor
    try:
        # They take 5, 10, 15,.. seconds to run.
        # The third job will throw and exception, and the 4th one will just call os.exit
        exceptions = [None, None, "bad_job", MockJob.DIE_COMMAND, None]

        logger.info('{}: Test five jobs starting loop'.format(os.getpid()))
        for i in [i for i in range(5)]:
            logger.info('{}: test loop qsub {}'.format(os.getpid(), "{root}/tests/mock_job.py".format(root=root)))
            q.qsub(
                runfile="{root}/tests/mock_job.py".format(root=root),
                jobname="mock_{}".format(i),
                parameters=["job_" + str(i), 5 * i, exceptions[i]],
                slots=15,
                memory=30,
                project="ihme_general")
        logger.info('{}: test_five_jobs waiting for qblock'.format(os.getpid()))
        q.qblock(poll_interval=60)  # monitor them
        logger.debug('    {}: test_five_jobs qblock complete'.format(os.getpid()))

        # query returns a dataframe
        failed = q.central_job_monitor_launcher.query(
            "select * from job where current_status != 5;"
        )
        number_failed = len(failed)
        logger.info('{}: Number of failed jobs {}'.format(os.getpid(), number_failed))

        for x in failed:
            logger.debug("   {}".format(x))

        # Check the 3 were good, and 2 died
        if number_failed != 2:
            raise Exception("Too many jobs failed ({}), expected 2 failures".format(number_failed))
    except Exception as e:
        logger.error("test_five_jobs failed with exception '{}'".format(e))
        assert False
    finally:
        logger.info('{}: test_five_jobs stopping the monitor'.format(os.getpid()))
        q.stop_monitor()  # stop monitor
