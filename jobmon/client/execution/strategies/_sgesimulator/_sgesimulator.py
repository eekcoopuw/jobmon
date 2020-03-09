from typing import List, Tuple, Dict, Optional

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor, TaskInstanceSGEInfo

logger = logging.getLogger(__name__)

""" The following classes are create for system testing purpose.
They mimic the sge executor without actually running SGE.
"""


class _SimulatorTaskInstanceSGEInfo(TaskInstanceSGEInfo):
    _autoincrease_counter = 0

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            _SimulatorTaskInstanceSGEInfo._autoincrease_counter += 1
            self._executor_id = _SimulatorTaskInstanceSGEInfo._autoincrease_counter
        return self._executor_id

    def get_usage_state(self) -> Dict:
        return {self._executor_id: {'usage_str': 'wallclock=03:30:11, cpu=00:00:00, mem=0.00000 GBs, io=0.00000 GB, '
                                                 'iow=0.000 s, ioops=0, vmem=N/A, maxvmem=N/A',
                                    'nodename': 'int-uge-archive-p003.cluster.ihme.washington.edu:1',
                                    'wallclock': 12611.0, 'cpu': '00:00:00',
                                    'mem': '0.00000 GBs',
                                    'io': '0.00000 GB',
                                    'iow': '0.000 s',
                                    'ioops': '0',
                                    'vmem': 'N/A',
                                    'maxvmem': 'N/A'}}


class _SimulatorSGEExecutor(SGEExecutor):
    _autoincrease_counter = 0
    def __init__(self,
                 stderr: Optional[str] = None,
                 stdout: Optional[str] = None,
                 project: Optional[str] = None,
                 working_dir: Optional[str] = None,
                 *args, **kwargs) -> None:
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir

        super().__init__(*args, **kwargs)

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        from jobmon.client.execution.worker_node.execution_wrapper import (
            unwrap, parse_arguments)
        from queue import Queue
        from unittest.mock import patch
        with patch("jobmon.client.execution.worker_node.execution_wrapper._get_executor_class") as m_get_executor_c, \
             patch("jobmon.client.execution.worker_node.execution_wrapper._run_in_sub_process") as m_run_in_sub_p:
            m_get_executor_c.return_value = "_SimulatorSGEExecutor", _SimulatorTaskInstanceSGEInfo()
            m_run_in_sub_p.return_value = Queue(), 0
            unwrap(**parse_arguments(command))
            _SimulatorSGEExecutor._autoincrease_counter += 1
        return _SimulatorSGEExecutor._autoincrease_counter


    def get_actual_submitted_or_running(self) -> List[int]:
        return []

    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        pass

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        msg = ("Task Instance killed because it exceeded max_runtime. "
               f"{self.__class__.__name__} accounting discovered exit "
               f"code:137.")
        return TaskInstanceStatus.RESOURCE_ERROR, msg
