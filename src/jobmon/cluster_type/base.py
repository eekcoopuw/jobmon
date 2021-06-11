"""Inteface definition for jobmon executor plugins."""

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Protocol, Tuple

from jobmon import __version__
from jobmon.exceptions import RemoteExitInfoNotAvailable


class ClusterQueue(Protocol):

    @abstractmethod
    def __init__(self, queue_id: int, queue_name: str, parameters: Dict):
        raise NotImplementedError

    @abstractmethod
    def validate_resource(self, resource: str, value: Any, fail=False) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def required_resources(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_id(self):
        raise NotImplementedError


class ClusterDistributor(Protocol):

    @property
    @abstractmethod
    def worker_node_wrapper_executable(self):
        """Path to jobmon worker node executable"""
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_type_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start the distributor."""
        raise NotImplementedError

    @abstractmethod
    def stop(self, distributor_ids: List[int]) -> None:
        """Stop the distributor."""
        raise NotImplementedError

    @abstractmethod
    def get_queueing_errors(self, distributor_ids: List[int]) -> Dict[int, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    @abstractmethod
    def get_submitted_or_running(self, distributor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    @abstractmethod
    def terminate_task_instances(self, distributor_ids: List[int]) -> None:
        """If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def get_remote_exit_info(self, distributor_ids: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

    @abstractmethod
    def submit_to_batch_distributor(self, command: str, name: str,
                                    requested_resources: Dict[str, Any]) -> int:
        """Submit the command on the cluster technology and return a distributor_id.

        The distributor_id can be used to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If an exception is raised by
        this method the task instance will move to "W" state and the exception will be logged
        in the database under the task_instance_error_log table.

        Args:
            command: command to be run
            name: name of task
            requested_resources: resource requests sent to distributor API
        """
        raise NotImplementedError

    def build_worker_node_command(self, task_instance_id: int) -> str:
        """Build a command that can be executed by the worker_node.

        Args:
            task_instance_id: id for the given instance of this task

        Returns:
            (str) unwrappable command
        """
        wrapped_cmd = [
            self.worker_node_wrapper_executable,
            "--task_instance_id", task_instance_id,
            "--expected_jobmon_version", __version__,
            "--cluster_type_name", self.cluster_type_name
        ]
        str_cmd = " ".join([str(i) for i in wrapped_cmd])
        return str_cmd


class ClusterWorkerNode(Protocol):
    """Base class defining interface for gathering executor specific info
    in the execution_wrapper.

    While not required, implementing get_usage_stats() will allow collection
    of CPU/memory utilization stats for each job.

    Get exit info is used to determine the error type if the task hits a
    system error of some variety.
    """

    @property
    @abstractmethod
    def distributor_id(self) -> Optional[int]:
        """Executor specific id assigned to a task instance."""
        raise NotImplementedError

    @abstractmethod
    def get_usage_stats(self) -> Dict:
        """Usage information specific to the exector."""
        raise NotImplementedError

    @abstractmethod
    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Error and exit code info from the executor."""
        raise NotImplementedError
