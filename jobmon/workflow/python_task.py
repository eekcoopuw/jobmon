import logging

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.models import JobStatus

logger = logging.getLogger(__name__)


class PythonTask(ExecutableTask):

    def __init__(self, path_to_python_binary="python", runfile=None, args=None,
                 slots=1, mem_free=2, max_attempts=1, max_runtime=None,
                 project=None, stderr=None, stdout=None, upstream_tasks=[]):
        self.command = PythonTask.make_cmd(path_to_python_binary, runfile,
                                           args)
        ExecutableTask.__init__(self, self.command,
                                upstream_tasks=upstream_tasks)
        self.slots = slots
        self.mem_free = mem_free
        self.max_attempts = max_attempts
        self.max_runtime = max_runtime
        self.project = project
        self.stderr = stderr
        self.stdout = stdout

    def make_cmd(path_to_python_binary, runfile, args):
        cmd = [path_to_python_binary, runfile]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)

    def bind(self, job_list_manager):
        """
        Args:
            job_list_manager: Used to create the Job

        Returns:
            The job_id of the new Job
        """
        logger.debug("Create job, command = {}".format(self.command))

        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.command,
            slots=self.slots,
            mem_free=self.mem_free,
            max_attempts=self.max_attempts,
            max_runtime=self.max_runtime,
            project=self.project,
            stderr=self.stderr,
            stdout=self.stdout)
        self.status = JobStatus.REGISTERED
        return self.job_id
