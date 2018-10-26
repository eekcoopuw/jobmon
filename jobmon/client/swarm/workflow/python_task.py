import logging
import sys

from jobmon.client.swarm.workflow.executable_task import ExecutableTask

logger = logging.getLogger(__name__)


class PythonTask(ExecutableTask):

    current_python = sys.executable

    def __init__(self, path_to_python_binary=current_python, script=None,
                 args=None, upstream_tasks=None, env_variables={}, name=None,
                 slots=None, num_cores=None, mem_free=2, max_attempts=3,
                 max_runtime_seconds=None, tag=None, queue=None, j_resource=False):
        """
        Args:
            path_to_python_binary (str): the python install that should be used
                Default is the Python install where Jobmon is installed
            script (str): the full path to the python code to run
            args (list): list of arguments to pass in to the script
            upstream_tasks (list): Task objects that must be run prior to this
            env_variables (dict): any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
            name (str): name that will be visible in qstat for this job
            slots (int): slots to request on the cluster. Default is 1
            num_cores (int): number of cores to request on the cluster
            mem_free (int): amount of memory in GBs to request on the cluster.
                Generally 2x slots. Default is 2
            max_attempts (int): number of attempts to allow the cluster to try
                before giving up. Default is 1
            max_runtime_seconds (int): how long the job should be allowed to
                run before the executor kills it. Default is None, for
                indefinite.
            tag (str): a group identifier. Currently just used for
                visualization. All tasks with the same tag will be colored the
                same in a TaskDagViz instance. Default is None.
            queue (str): queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            j_resource (bool): Whether or not this task uses the j_drive

        """
        self.command = PythonTask.make_cmd(path_to_python_binary, script,
                                           args)
        super(PythonTask, self).__init__(
            command=self.command, env_variables=env_variables,
            upstream_tasks=upstream_tasks, name=name, slots=slots,
            num_cores=num_cores, mem_free=mem_free, max_attempts=max_attempts,
            max_runtime_seconds=max_runtime_seconds, tag=tag, queue=queue,
            j_resource=j_resource)

    @staticmethod
    def make_cmd(path_to_python_binary, script, args):
        cmd = [path_to_python_binary, script]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)
