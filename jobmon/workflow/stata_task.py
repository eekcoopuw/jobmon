import logging

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.models import JobStatus

logger = logging.getLogger(__name__)


class StataTask(ExecutableTask):

    # The command "stata-mp" is considered better than the simple command "stata" because it allows you to use
    # more than one core.
    default_stata_script = "stata-mp"

    def __init__(self, path_to_stata_binary=default_stata_script, script=None,
                 args=None, **kwargs):
        """
        This will run a stata file using stata-mp command, using the flags -b (batch) and -q (quiet).
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
            upstream_tasks (list): Task objects that must be run prior to this
        """
        self.command = StataTask.make_cmd(path_to_stata_binary, script,
                                           args)
        super(StataTask, self).__init__(command=self.command, **kwargs)

    @staticmethod
    def make_cmd(path_to_stata_binary, script, args):
        cmd = [path_to_stata_binary, '-q', '-b', script]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)
