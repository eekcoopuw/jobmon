import os
import subprocess
import traceback
from typing import Optional

from jobmon.client.swarm.executors import (Executor, TaskInstanceExecutorInfo,
                                           ExecutorParameters)
from jobmon.client import shared_requester
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.client.swarm import SwarmLogging as logging

logger = logging.getLogger(__name__)


class SequentialExecutor(Executor):

    def __init__(self, stderr=None, stdout=None, project=None,
                 working_dir=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_executor_id = 1
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        try:
            logger.debug(command)

            # add an executor id to the environment
            env = os.environ.copy()
            executor_id = self._next_executor_id
            env["JOB_ID"] = str(self._next_executor_id)
            self._next_executor_id += 1

            # submit the job
            subprocess.check_output(command, shell=True, env=env)
        except Exception as e:
            logger.error(e)
            stack = traceback.format_exc()
            msg = (
                f"Error in {self.__class__.__name__}, {str(self)} "
                f"while running {command}:"
                f"\n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")
        return executor_id


class TaskInstanceSequentialInfo(TaskInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            tid = os.environ.get('JOB_ID')
            if tid:
                self._executor_id = int(tid)
        return self._executor_id

    def get_exit_info(self, exit_code, error_msg):
        return TaskInstanceStatus.ERROR, error_msg
