import logging
import traceback
import subprocess

from jobmon.client.swarm.executors import Executor
from jobmon.client import shared_requester


logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

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
