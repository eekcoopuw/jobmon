import logging
import os

import jobmon.workflow.task as tk

logger = logging.getLogger(__name__)


class SleepAndWriteFileMockTask(tk.ExecutableTask):
    """
    A simple task that sleeps for a confgiured numebr of seconds, then writes a File.
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
        tk.ExecutableTask.__init__(self, SleepAndWriteFileMockTask.construct_logical_name(output_file_name),upstream_tasks=upstream_tasks )
        # TBD validation using the types module.
        self.sleep_secs = sleep_secs
        self.output_file_name = output_file_name
        self.fail_always = fail_always

        # Needed by Job
        # NB this package must be installed into the conda env so that it will be found
        logger.debug("attempting remote call")
        self.command = "remote_sleep_and_write --sleep_secs {s} --output_file_path {ofn} --name {n}". \
            format(s=self.sleep_secs, ofn=self.output_file_name, n=self.logical_name)
        if self.fail_always:
            self.command += " --fail_always"

    @staticmethod
    def construct_logical_name(output_file_name=None):
        basename = os.path.basename( output_file_name )
        return "{}_mock_task_{}_".format(basename, output_file_name).replace("/","_")

    def create_job(self, job_list_manager):
        # In future it would be nice to have test instance of the central jobmon server, so we would need jobmon_params
        # job_params = job_params + self.jobmon_params
        logger.debug("command = {}".format(self.command))

        self.job_id = job_list_manager.create_job(
            jobname=self.logical_name,
            command=self.command,
            slots=1,
            mem_free=2
            # stderr="{}/stderr/stderr-$JOB_ID-mock-test.txt".format(os.path.basename(self.output_file_name)),
            # project="proj_burdenator",  # TBD which project?,
            # process_timeout=30
        )
        return self.job_id

    def queue_job(self, job_list_manager):
        """
        TBD Hoist to super class?
        :param job_list_manager:
        :return:
        """
        job_list_manager.queue_job(self.job_id)
        return self.job_id
