import logging
import os

from jobmon.client.swarm.workflow.executable_task import ExecutableTask

logger = logging.getLogger(__name__)


class StataTask(ExecutableTask):

    # The command "stata-mp" is considered better than the simple command
    # "stata" because it allows you to use more than one core.
    default_stata_script = "stata-mp"

    def __init__(self, path_to_stata_binary=default_stata_script, script=None,
                 args=None, upstream_tasks=None, env_variables={}, name=None,
                 slots=1, num_cores=None, mem_free_gb=2, max_attempts=3,
                 max_runtime_seconds=None, tag=None, queue=None, j_resource=False):
        """
        This runs a stata file using stata-mp command, using the flags -b
        (batch) and -q (quiet).
        It writes a stata log file in the root directory where the process
        executes, which is the home directory of the user who is running jobmon

        Args:
            path_to_stata_binary (str): the Stata install that should be used
                Default is the cluster's stata install, which was
                /usr/local/bin/stata in Jan 2018
            script (str): the full path to the python code to run
            args (list): list of arguments to pass in to the script
            upstream_tasks (list): Task objects that must be run prior to this
            env_variables (dict): any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
            name (str): name that will be visible in qstat for this job
            slots (int): slots to request on the cluster. Default is 1
            mem_free (int): amount of memory in GBs to request on the cluster.
                Generally 2x slots. Default is 2
            max_attempts (int): number of attempts to allow the cluster to try
                before giving up. Default is 1
            max_runtime (int, seconds): how long the job should be allowed to
                run before the executor kills it. Default is None, for
                indefinite.
            tag (str): a group identifier. Currently just used for
                visualization. All tasks with the same tag will be colored the
                same in a TaskDagViz instance. Default is None.
            queue (str): queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            j_resource (bool): whether or not this task uses the j_drive
        """
        self.command = StataTask.make_cmd(path_to_stata_binary, script, args)
        super(StataTask, self).__init__(
            command=self.command, env_variables=env_variables,
            upstream_tasks=upstream_tasks, name=name, slots=slots,
            num_cores=num_cores, mem_free_gb=mem_free_gb, max_attempts=max_attempts,
            max_runtime_seconds=max_runtime_seconds, tag=tag, queue=queue,
            j_resource=j_resource)

    @staticmethod
    def make_cmd(path_to_stata_binary, script, args):
        script_fn = os.path.basename(script)
        symlink_dst = ("\$JOBMON_TEMP_DIR/\$JOBMON_JOB_INSTANCE_ID-"
                       "{script}".format(script=script_fn))
        cmd = [
            "cd", "\$JOBMON_TEMP_DIR", "&&",
            "ln", "-s", script, symlink_dst, "&&",
            path_to_stata_binary, '-q', '-b', symlink_dst]
        if args:
            cmd.append(' '.join([str(x) for x in args]))
        return ' '.join(cmd)
