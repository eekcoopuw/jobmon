import logging
import os

import jobmon.workflow.executable_task as etk

logger = logging.getLogger(__name__)


class SleepAndWriteFileMockTask(etk.ExecutableTask):
    """
    A simple task that sleeps for a configured number of seconds, then writes a File.
    It can also be command to alwyas fail (by raising ValueError), or fail a certain number of times and then succeed.

    The actual SGE command is remote_sleep_and_write.py
    """

    def __init__(self,
                 sleep_secs=3,
                 output_file_name=None,
                 upstream_tasks=None,
                 fail_always=False,
                 fail_count=0
                 ):
        etk.ExecutableTask.__init__(self, SleepAndWriteFileMockTask.construct_hash_name(output_file_name),
                                    upstream_tasks=upstream_tasks)
        # TBD validation using the types module.
        self.sleep_secs = sleep_secs
        self.output_file_name = output_file_name
        self.fail_always = fail_always
        self.fail_count = fail_count

        # NB this package must be installed into the conda env so that it will be found
        logger.debug("attempting remote call")
        self.command = "remote_sleep_and_write --sleep_secs {s} --output_file_path {ofn} --name {n}". \
            format(s=self.sleep_secs, ofn=self.output_file_name, n=self.hash_name)
        if self.fail_always:
            self.command += " --fail_always"
        if self.fail_count:
            self.command += " --fail_count {}".format(fail_count)

    @staticmethod
    def construct_hash_name(output_file_name):
        basename = os.path.basename(output_file_name)
        return "{}_mock_task_{}_".format(basename, output_file_name).replace("/", "_")

    def create_job(self, job_list_manager):
        """
        Creates the SGE Job

        Args:
            job_list_manager:

        Returns:
          the job_id
        """
        logger.debug("command = {}".format(self.command))

        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            command=self.command,
            slots=1,
            mem_free=2,
            max_attempts=3,
            # TBD Project and stderr
            # stderr="{}/stderr/stderr-$JOB_ID-mock-test.txt".format(os.path.basename(self.output_file_name)),
            # project="proj_burdenator",  # TBD which project?,
            # process_timeout=30
        )
        return self.job_id
