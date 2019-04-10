import logging
import os
import shutil

from jobmon.client import client_config


logger = logging.getLogger(__name__)


class Executor(object):
    """Base class for executors. Subclasses are required to implement an
    execute() method that takes a JobInstance, constructs a
    jobmon-interpretable executable command (typically using this base class's
    build_wrapped_command()), and optionally returns an executor_id.

    While not required, implementing get_usage_stats() will allow collection
    of CPU/memory utilization stats for each job.

    Also optional, get_actual_submitted_or_running() and
    terminate_job_instances() are recommended in case jobs fail in ways
    that they are unable to contact Jobmon re: the reasons for their failure.
    These methods will allow jobmon to identify jobs that have been lost
    and retry them.
    """

    def __init__(self, *args, **kwargs):
        self.temp_dir = None
        logger.info("Initializing {}".format(self.__class__.__name__))

    def execute(self, job_instance):
        """SUBCLASSES ARE REQUIRED TO IMPLEMENT THIS METHOD.

        It is recommended that subclasses use build_wrapped_command() to
        generate the executable command string itself. It is then up to the
        Executor subclass to provide a means of actually executing that
        command.

        Optionally, return an (int) executor_id which the subclass could
        use at a later time to identify the associated JobInstance, terminate
        it, monitor for missingness, or collect usage statistics. If the
        subclass does not intend to offer those functionalities, this method
        can return None.
        """
        raise NotImplementedError

    def get_usage_stats(self):
        raise NotImplementedError

    def get_actual_submitted_or_running(self):
        raise NotImplementedError

    def get_actual_submitted_to_executor(self):
        raise NotImplementedError

    def terminate_job_instances(self, job_instance_list):
        """If implemented, return a list of (job_instance_id, hostname) tuples
        for any job_instances that are terminated
        """
        raise NotImplementedError

    def build_wrapped_command(self, job, job_instance_id):
        """Build a command that can be executed by the shell and can be
        unwrapped by jobmon itself to setup proper communication channels to
        the monitor server.

        Args:
            job (job.Job): the job to be run
            job_instance_id (int): the id of the job_instance to be run

        Returns:
            (str) unwrappable command
        """
        jobmon_command = client_config.jobmon_command
        if not jobmon_command:
            jobmon_command = shutil.which("jobmon_command")
        wrapped_cmd = [
            jobmon_command,
            "--command", "'{}'".format(job.command),
            "--job_instance_id", job_instance_id,
            "--jm_host", client_config.jm_conn.host,
            "--jm_port", client_config.jm_conn.port,
            "--executor_class", self.__class__.__name__,
            "--heartbeat_interval", client_config.heartbeat_interval
        ]
        if self.temp_dir and 'stata' in job.command:
            wrapped_cmd.extend(["--temp_dir", self.temp_dir])
        if job.last_nodename:
            wrapped_cmd.extend(["--last_nodename", job.last_nodename])
        if job.last_process_group_id:
            wrapped_cmd.extend(["--last_pgid", job.last_process_group_id])
        wrapped_cmd = " ".join([str(i) for i in wrapped_cmd])
        logger.debug(wrapped_cmd)
        return wrapped_cmd

    def set_temp_dir(self, temp_dir):
        self.temp_dir = temp_dir
        os.environ["JOBMON_TEMP_DIR"] = self.temp_dir
