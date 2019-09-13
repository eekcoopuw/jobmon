import logging
import os
from typing import Optional, List

from jobmon.execution.strategies import (Executor, JobInstanceExecutorInfo,
                                         ExecutorParameters)
from jobmon.execution.worker_node.execution_wrapper import (unwrap,
                                                            parse_arguments)
from jobmon.models.job_instance_status import JobInstanceStatus

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

    def start(self):
        pass

    def stop(self):
        pass

    def get_actual_submitted_or_running(self) -> List[int]:
        running = os.environ.get("JOB_ID")
        if running:
            return [int(running)]
        else:
            return []

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        logger.debug(command)

        # add an executor id to the environment
        os.environ["JOB_ID"] = self._next_executor_id
        executor_id = self._next_executor_id
        self._next_executor_id += 1

        # run the job
        unwrap(**parse_arguments(command))
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
