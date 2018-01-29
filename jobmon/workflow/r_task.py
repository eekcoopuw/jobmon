import logging

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.models import JobStatus

logger = logging.getLogger(__name__)


class RTask(ExecutableTask):

    # R and Rscript are reliably installed on the cluster
    default_R_script = "Rscript"

    def __init__(self, path_to_R_binary=default_R_script, script=None,
                 args=None, **kwargs):
        """
        Args:
            path_to_R_binary (str): the R install that should be used
                Default is the cluster's R, which was /usr/local/bin/R in Jan 2018
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
        self.command = RTask.make_cmd(path_to_R_binary, script,
                                           args)
        super(RTask, self).__init__(command=self.command, **kwargs)

    @staticmethod
    def make_cmd(path_to_R_binary, script, args):
        cmd = [path_to_R_binary, script]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)
