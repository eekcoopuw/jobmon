import logging
import os
import traceback
import subprocess

from jobmon.client.swarm.executors import Executor, ExecutorWorkerNode
from jobmon.client import shared_requester
from jobmon.models.job_instance_status import JobInstanceStatus

logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_executor_id = 1

    def execute(self, job_instance):
        try:
            cmd = self.build_wrapped_command(job_instance.job,
                                             job_instance.job_instance_id)
            logger.debug(cmd)

            # add an executor id to the environment
            env = os.environ.copy()
            env["JOB_ID"] = str(self._next_executor_id)
            self._next_executor_id += 1

            # submit the job
            subprocess.check_output(cmd, shell=True, env=env)
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


class SequentialExecutorWorkerNode(ExecutorWorkerNode):

    @property
    def executor_id(self):
        if self._executor_id is None:
            self._executor_id = os.environ.get('JOB_ID')
        return self._executor_id

    def get_exit_info(self, exit_code, error_msg):
        return JobInstanceStatus.ERROR, error_msg
