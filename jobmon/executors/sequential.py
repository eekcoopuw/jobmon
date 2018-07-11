import logging
import subprocess

from jobmon.command_context import build_wrapped_command
from jobmon.executors import Executor


logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

    def execute(job_instance):
        try:
            cmd = build_wrapped_command(job_instance.job,
                                        job_instance.job_instance_id)
            logger.debug(cmd)
            subprocess.check_output(cmd, shell=True)
        except Exception as e:
            logger.error(e)
        return None
