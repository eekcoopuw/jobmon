import logging
import os

import jobmon.workflow.executable_task as etk

logger = logging.getLogger(__name__)


class SleepAndWriteFileMockTask(etk.ExecutableTask):
    """
    A simple task that sleeps for a configured number of seconds, then writes a
    File. It can also be command to always fail (by raising ValueError), or
    fail a certain number of times and then succeed.

    The actual SGE command is remote_sleep_and_write.py
    """

    def __init__(self,
                 command,
                 upstream_tasks=None,
                 fail_always=False,
                 fail_count=0
                 ):
        etk.ExecutableTask.__init__(self, command,
                                    upstream_tasks=upstream_tasks)
        # TBD validation using the types module.
        self.fail_always = fail_always
        self.fail_count = fail_count

        # NB this package must be installed into the conda env so that it will
        # be found
        self.output_file_name = command.split(
            '--output_file_path ')[1].split(" ")[0]
        self.command = command
        if self.fail_always:
            self.command += " --fail_always"
        if self.fail_count:
            self.command += " --fail_count {}".format(fail_count)

    def create_job(self, job_list_manager):
        """
        Creates the SGE Job

        Args:
            job_list_manager:

        Returns:
          the job_id
        """
        logger.debug("Create job, command = {}".format(self.command))
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            command=self.command,
            job_hash=self.hash,
            slots=1,
            mem_free=2,
            max_attempts=3,
            project='proj_jenkins',
            stderr=("{}/stderr/stderr-$JOB_ID-mock-test.txt"
                    .format(os.path.dirname(self.output_file_name))),
            stdout=("{}/stdout/stdout-$JOB_ID-mock-test.txt"
                    .format(os.path.dirname(self.output_file_name))))
        return self.job_id
