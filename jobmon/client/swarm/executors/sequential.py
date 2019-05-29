import logging
import subprocess
import traceback

from jobmon.client import shared_requester
from jobmon.client.swarm.executors import Executor

logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

    def __init__(self, stderr=None, stdout=None, project=None,
                 working_dir=None, *args, **kwargs):
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir
        super().__init__(*args, **kwargs)

    def execute(self, job_instance):
        try:
            cmd = self.build_wrapped_command(job_instance.job,
                                             job_instance.job_instance_id)
            logger.debug(cmd)
            subprocess.check_output(cmd, shell=True)
        except Exception as e:
            logger.error(e)
            stack = traceback.format_exc()
            msg = (
                f"Error in {self.__class__.__name__}, {str(self)} "
                f"while submitting ji_id {job_instance.job_instance_id}:"
                f"\n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")
        return None
