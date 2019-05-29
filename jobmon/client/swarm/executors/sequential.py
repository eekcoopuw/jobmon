import logging
import os
import subprocess
import traceback
from typing import Optional

from jobmon.client.swarm.executors import Executor, JobInstanceExecutorInfo
from jobmon.client import shared_requester
from jobmon.client.swarm.job_management.executor_job import ExecutorJob


logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_executor_id = 1

    def execute(self, job: ExecutorJob, job_instance_id: int) -> int:
        try:
            cmd = self.build_wrapped_command(job, job_instance_id)
            logger.debug(cmd)

            # add an executor id to the environment
            env = os.environ.copy()
            executor_id = self._next_executor_id
            env["JOB_ID"] = str(self._next_executor_id)
            self._next_executor_id += 1

            # submit the job
            subprocess.check_output(cmd, shell=True, env=env)
        except Exception as e:
            logger.error(e)
            stack = traceback.format_exc()
            msg = (
                f"Error in {self.__class__.__name__}, {str(self)} "
                f"while submitting ji_id {job_instance_id}:"
                f"\n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")
        return executor_id


class JobInstanceSequentialInfo(JobInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._executor_id = int(jid)
        return self._executor_id

    def get_exit_info(self, exit_code, error_msg):
        return JobInstanceStatus.ERROR, error_msg
