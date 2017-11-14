import logging
import hashlib

import jobmon.workflow.executable_task as etk

logger = logging.getLogger(__name__)


class BashTask(etk.ExecutableTask):
    def __init__(self, command, upstream_tasks=[]):
        etk.ExecutableTask.__init__(
            self, hash_name=BashTask.construct_hash_name(command),
            upstream_tasks=upstream_tasks)
        self.command = command

    @staticmethod
    def construct_hash_name(command):
        """ Hashes the string itself, to eliminate all invalid sge characters.
        Note: can't use builtin hash function, as it's randomly generated upon
        the startup of each python process, and is not reproducible
        """
        return hashlib.sha1(command).hexdigest()

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
            slots=1,
            mem_free=2,
            max_attempts=3
        )
        return self.job_id
