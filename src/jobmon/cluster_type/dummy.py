"""Dummy Executor fakes execution for testing purposes."""
import logging
import random
from typing import Any, Dict, List, Tuple


from jobmon.cluster_type.base import ClusterDistributor
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.worker_node.cli import WorkerNodeCLI
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


logger = logging.getLogger(__name__)


class DummyDistributor(ClusterDistributor):
    """The Dummy Executor fakes the execution of a Task and acts as though it succeeded."""

    def __init__(self):
        self.started = False
        # Parse the config
        worker_node_config = WorkerNodeConfig.from_defaults()
        self.heartbeat_report_by_buffer = \
            worker_node_config.heartbeat_report_by_buffer
        self.task_instance_heartbeat_interval = \
            worker_node_config.task_instance_heartbeat_interval

    @property
    def worker_node_wrapper_executable(self):
        """Path to jobmon worker node executable"""
        return ""

    @property
    def cluster_type_name(self) -> str:
        return "Dummy"

    def start(self) -> None:
        """Start the executor."""
        self.started = True

    def stop(self, executor_ids: List[int]) -> None:
        """Stop the executor."""
        self.started = False

    def execute(self, command: str, name: str, requested_resources: Dict[str, Any]) -> int:
        """Run a fake execution of the task.
        in a real executor, this is where qsub would happen.
        here, since it's a dummy executor, we just get a random num
        """
        logger.debug("This is the Dummy Distributor")
        executor_id = random.randint(1, int(1e7))

        cli = WorkerNodeCLI()
        args = cli.parse_args(command)

        # Bring in the worker node here since dummy executor is never run
        worker_node_task_instance = WorkerNodeTaskInstance(
            task_instance_id=args.task_instance_id,
            expected_jobmon_version=args.expected_jobmon_version,
            cluster_type_name=args.cluster_type_name
        )

        # Log running, log done, and exit
        _, _, _ = worker_node_task_instance.log_running(
            self.heartbeat_report_by_buffer * self.task_instance_heartbeat_interval)
        worker_node_task_instance.log_done()

        return executor_id

    def get_queueing_errors(self, executor_ids: List[int]) -> Dict[int, str]:
        """Dummy tasks never error, since they never run.
        So always return an empty dict"""
        return {}

    def get_submitted_or_running(self, executor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        """
        No such thing as running Dummy tasks. Therefore, nothing to terminate.
        """
        raise NotImplementedError

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable
