"""Dummy Executor fakes execution for testing purposes."""
import os
import logging
import random
from typing import Optional, Any, Dict, List, Tuple, Type

from jobmon.cluster_type.base import ClusterDistributor, ClusterWorkerNode, ClusterQueue
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.worker_node.cli import WorkerNodeCLI
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


logger = logging.getLogger(__name__)

class DummyQueue(ClusterQueue):

    def __init__(self, queue_id: int, queue_name: str, parameters: dict):

        # Get the limits from DB in client
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resource(self, resource, value: Any, fail=False):
        """Ensure cores requested isn't more than available on that
        node.
        """
        return ""

    @property
    def queue_id(self):
        return self._queue_id

    @property
    def queue_name(self):
        return self._queue_name

    @property
    def parameters(self):
        return self._parameters

    @property
    def required_resources(self):
        return []

class DummyDistributor(ClusterDistributor):
    """The Dummy Executor fakes the execution of a Task and acts as though it succeeded."""

    def __init__(self):
        self.started = False
        self._worker_node_entry_point = shutil.which("worker_node_entry_point")
        # Parse the config
        worker_node_config = WorkerNodeConfig.from_defaults()
        self.heartbeat_report_by_buffer = \
            worker_node_config.heartbeat_report_by_buffer
        self.task_instance_heartbeat_interval = \
            worker_node_config.task_instance_heartbeat_interval

    @property
    def worker_node_entry_point(self):
        """Path to jobmon worker_node_entry_point"""
        return self._worker_node_entry_point

    @property
    def cluster_type_name(self) -> str:
        return "dummy"

    def start(self) -> None:
        """Start the executor."""
        self.started = True

    def stop(self, distributor_ids: List[int]) -> None:
        """Stop the executor."""
        self.started = False


    def get_queueing_errors(self, distributor_ids: List[int]) -> Dict[int, str]:
        """Dummy tasks never error, since they never run.
        So always return an empty dict"""
        return {}

    def get_submitted_or_running(self, distributor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    def terminate_task_instances(self, distributor_ids: List[int]) -> None:
        """
        No such thing as running Dummy tasks. Therefore, nothing to terminate.
        """
        raise NotImplementedError

    def submit_to_batch_distributor(self, command: str, name: str,
                                    requested_resources: Dict[str, Any]) -> int:
        """Run a fake execution of the task.
        in a real executor, this is where qsub would happen.
        here, since it's a dummy executor, we just get a random num
        """
        logger.debug("This is the Dummy Distributor")
        distributor_id = random.randint(1, int(1e7))

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

        return distributor_id

    def get_remote_exit_info(self, distributor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

class DummyWorkerNode(ClusterWorkerNode):
    """Get Executor Info for a Task Instance."""

    def __init__(self) -> None:
        self._distributor_id: Optional[int] = None

    @property
    def distributor_id(self) -> Optional[int]:
        """Executor id of the task."""
        if self._distributor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._distributor_id = int(jid)
        return self._distributor_id

    def get_exit_info(self, exit_code: int, error_msg: str):
        """Exit info, error message."""
        return TaskInstanceStatus.ERROR, error_msg

    def get_usage_stats(self) -> Dict:
        """Usage information specific to the exector."""
        return {}

def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.cluster_type.sequential.seq_client import SequentialQueue
    return DummyQueue

def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    return DummyDistributor

def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    return DummyWorkerNode
