import logging
import subprocess

from jobmon.config import config


logger = logging.getLogger(__name__)


class Executor(object):

    def __init__(*args, **kwargs):
        raise NotImplementedError

    def execute(job_instance):
        raise NotImplementedError

    def get_usage_stats(job_id):
        raise NotImplementedError

    def get_actual_submitted_or_running():
        raise NotImplementedError

    def terminate_job_instances(job_instance_list):
        """If implemented, return a list of (job_instance_id, hostname) tuples
        for any job_instances that are terminated"""
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
        jobmon_command = config.default_opts.get('jobmon_command', None)
        if not jobmon_command:
            jobmon_command = subprocess.check_output(
                ["which", "jobmon_command"])
        jobmon_command = jobmon_command.strip().decode("utf-8")
        wrapped_cmd = [
            jobmon_command,
            "--command", "'{}'".format(job.command),
            "--job_instance_id", job_instance_id,
            "--jsm_host", config.jm_rep_conn.host,
            "--jsm_port", config.jm_rep_conn.port,
            "--executor_class", self.__class__.__name__,
        ]
        if job.last_nodename:
            wrapped_cmd.extend(["--last_nodename", job.last_nodename])
        if job.last_process_group_id:
            wrapped_cmd.extend(["--last_pgid", job.last_process_group_id])
        wrapped_cmd = " ".join([str(i) for i in wrapped_cmd])
        logger.debug(wrapped_cmd)
        return wrapped_cmd
