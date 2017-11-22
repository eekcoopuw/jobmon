import logging

from jobmon.workflow.executable_task import ExecutableTask

logger = logging.getLogger(__name__)


class BashTask(ExecutableTask):
    def __init__(self, command, upstream_tasks=[]):
        ExecutableTask.__init__(
            self, command, upstream_tasks=upstream_tasks)
        self.command = command

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
