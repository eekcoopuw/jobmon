import logging

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.models import JobStatus

logger = logging.getLogger(__name__)


class StataTask(ExecutableTask):

    # The command "stata-mp" is considered better than the simple command "stata" because it allows you to use
    # more than one core.
    default_stata_script = "stata-mp"

    def __init__(self, path_to_stata_binary=default_stata_script, script=None,
                 args=None, slots=1, mem_free=2, max_attempts=1,
                 max_runtime=None, project=None, stderr=None, stdout=None,
                 upstream_tasks=[]):
        """
        This will run a stata file using stata-mp command, using the flags -b (bacth) and -q (quiet).
        It will write a stata log file in the root directory where the process executes,
        which is the home directory of the user who is running jobmon.

        Args:
            path_to_stata_binary (str): the Stata install that should be used
                Default is the cluster's stata install, which was /usr/local/bin/stata in Jan 2018
            script (str): the full path to the python code to run
            args (list): list of arguments to pass in to the script
            slots (int): slots to request on the cluster. Default is 1
            mem_free (int): amount of memory to request on the cluster.
                Generally 2x slots. Default is 2
            max_attempts (int): number of attempts to allow the cluster to try
                before giving up. Default is 1
            max_runtime (int, seconds): how long the job should be allowed to
                run before having sge kill it. Default is None, for indefinite.
            project (str): cluster project to run job under
            stderr (str): filepath for where stderr should be saved
            stdout (str): filepath for where stdout should be saved
            upstream_tasks (list): Task objects that must be run prior to this
        """
        self.command = StataTask.make_cmd(path_to_stata_binary, script,
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

    def make_cmd(path_to_stata_binary, script, args):
        cmd = [path_to_stata_binary, '-q', '-b', script]
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
