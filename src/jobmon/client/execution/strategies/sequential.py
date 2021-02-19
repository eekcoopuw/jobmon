"""Sequential executor runs one task at a time."""
import os
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from jobmon.client.execution.strategies.base import (Executor, ExecutorParameters,
                                                     TaskInstanceExecutorInfo)
from jobmon.client.execution.worker_node.execution_wrapper import (
    parse_arguments, unwrap)
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import RemoteExitInfoNotAvailable

import structlog as logging


logger = logging.getLogger(__name__)


class LimitedSizeDict(OrderedDict):
    """Dictionary for exit info."""

    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value):
        """Set item in dict."""
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class SequentialExecutor(Executor):
    """Executor to run tasks one at a time."""

    def __init__(self, exit_info_queue_size: int = 1000, *args, **kwargs):
        """
        Args:
            exit_info_queue_size: how many exit codes to retain
        """
        super().__init__(*args, **kwargs)
        self._next_executor_id = 1
        self._exit_info = LimitedSizeDict(size_limit=exit_info_queue_size)

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """Get exit info from task instances that have run."""
        try:
            exit_code = self._exit_info[executor_id]
            if exit_code == 199:
                msg = "job was in kill self state"
                return TaskInstanceStatus.UNKNOWN_ERROR, msg
            else:
                return TaskInstanceStatus.UNKNOWN_ERROR, f"found {exit_code}"
        except KeyError:
            raise RemoteExitInfoNotAvailable

    def get_actual_submitted_or_running(self, executor_ids, report_by_buffer) -> \
            Tuple[List[int], Dict[int, int]]:
        """Check status of running task."""
        running = os.environ.get("JOB_ID")
        if running:
            return [int(running)], executor_ids
        else:
            return [], executor_ids

    def execute(self, command: str, name: str, executor_parameters: ExecutorParameters,
                executor_ids) -> Tuple[int, Dict[int, int]]:
        """Execute sequentially."""
        logger.debug(command)

        # add an executor id to the environment
        os.environ["JOB_ID"] = str(self._next_executor_id)
        executor_id = self._next_executor_id
        self._next_executor_id += 1

        # run the job and log the exit code
        try:
            exit_code = unwrap(**parse_arguments(command))
        except SystemExit as e:
            if e.code == 199:
                exit_code = e.code
            else:
                raise

        self._exit_info[executor_id] = exit_code
        return executor_id, executor_ids


class TaskInstanceSequentialInfo(TaskInstanceExecutorInfo):
    """Get Executor Info for a Task Instance."""

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        """Executor id of the task."""
        if self._executor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._executor_id = int(jid)
        return self._executor_id

    def get_exit_info(self, exit_code: int, error_msg: str):
        """Exit info, error message."""
        return TaskInstanceStatus.ERROR, error_msg
