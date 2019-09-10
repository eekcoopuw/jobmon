import logging
import sys
from typing import Optional

from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.client.swarm.executors.base import ExecutorParameters

logger = logging.getLogger(__name__)


class PythonTask(ExecutableTask):

    current_python = sys.executable

    def __init__(self, path_to_python_binary=current_python, script=None,
                 args=None, upstream_tasks=None, env_variables={}, name=None,
                 num_cores=None, max_attempts=3,
                 max_runtime_seconds=None, tag=None, queue=None,
                 j_resource=False, m_mem_free=None, context_args=None,
                 resource_scales=None, hard_limits=False,
                 executor_class='SGEExecutor',
                 executor_parameters: Optional[ExecutorParameters] = None):
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
            num_cores (int): number of cores to request on the cluster
            m_mem_free (str): amount of memory in gbs, tbs, or mbs (G, T, or M)
                to request on the fair cluster.
            max_attempts (int): number of attempts to allow the cluster to try
                before giving up. Default is 3
            max_runtime_seconds (int): how long the job should be allowed to
                run before the executor kills it. Default is None, for
                indefinite.
            tag (str): a group identifier. Currently just used for
                visualization. All tasks with the same tag will be colored the
                same in a TaskDagViz instance. Default is None.
            queue (str): queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            j_resource (bool): Whether or not this task uses the j_drive
            context_args (dict): Additional arguments to pass along with to the
                executor
            resource_scales(dict): for each resource, a scaling value (between 0 and 1)
                can be provided so that different resources get scaled differently.
                Default is {'m_mem_free': 0.5, 'max_runtime_seconds': 0.5}
            hard_limits: if the user wants jobs to stay on the chosen queue
                and not expand if resources are exceeded, set this to true
            executor_class (str): Executor class to configure the given
                parameters ex. SGEExecutor
            executor_parameters(ExecutorParameters): the set of executor
                specific parameters for the given task

        """
        self.command = PythonTask.make_cmd(path_to_python_binary, script,
                                           args)
        super(PythonTask, self).__init__(
            command=self.command, env_variables=env_variables,
            upstream_tasks=upstream_tasks, name=name,
            num_cores=num_cores, max_attempts=max_attempts,
            max_runtime_seconds=max_runtime_seconds, tag=tag, queue=queue,
            j_resource=j_resource, m_mem_free=m_mem_free,
            context_args=context_args, resource_scales=resource_scales,
            hard_limits=hard_limits, executor_class=executor_class,
            executor_parameters=executor_parameters)

    @staticmethod
    def make_cmd(path_to_python_binary, script, args):
        cmd = [path_to_python_binary, script]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)
