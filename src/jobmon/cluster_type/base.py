"""Inteface definition for jobmon executor plugins."""
from __future__ import annotations

from abc import abstractmethod
import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

# the following try-except is to accommodate Python versions on both >=3.8 and 3.7.
# The Protocol was officially introduced in 3.8, with typing_extensions slapped on 3.7.
try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable  # type: ignore

from jobmon import __version__
from jobmon.exceptions import RemoteExitInfoNotAvailable


class ClusterQueue(Protocol):
    """The protocol class for queues on a cluster."""

    @abstractmethod
    def __init__(self, queue_id: int, queue_name: str, parameters: Dict) -> None:
        """Initialization of ClusterQueue."""
        raise NotImplementedError

    @abstractmethod
    def validate_resources(self, **kwargs: Dict) -> Tuple[bool, str, Dict]:
        """Ensures that requested resources aren't greater than what's available."""
        raise NotImplementedError

    @property
    @abstractmethod
    def parameters(self) -> Dict:
        """Returns the dictionary of parameters."""
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_name(self) -> str:
        """Returns the name of the queue."""
        raise NotImplementedError

    @property
    @abstractmethod
    def queue_id(self) -> int:
        """Returns the ID of the queue."""
        raise NotImplementedError

    @property
    @abstractmethod
    def required_resources(self) -> List:
        """Returns the list of resources that are required."""
        raise NotImplementedError


class ClusterDistributor(Protocol):
    """The protocol class for cluster distributors."""

    @property
    @abstractmethod
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_type_name(self) -> str:
        """Return the name of the cluster type."""
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
        """Terminate task instances.

        If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def get_remote_exit_info(self, distributor_ids: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

    @abstractmethod
    def submit_to_batch_distributor(
        self, command: str, name: str, requested_resources: Dict[str, Any]
    ) -> int:
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
            "worker_node",
            "--task_instance_id",
            task_instance_id,
            "--expected_jobmon_version",
            __version__,
            "--cluster_type_name",
            self.cluster_type_name,
        ]
        str_cmd = " ".join([str(i) for i in wrapped_cmd])
        return str_cmd


class ClusterWorkerNode(Protocol):
    """Base class defining interface for gathering executor info in the execution_wrapper.

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


@runtime_checkable
class ConcreteResource(Protocol):
    """The protocol class for concrete resources."""

    @property
    @abstractmethod
    def queue(self) -> ClusterQueue:
        """The queue that these resources have been validated against."""
        raise NotImplementedError

    @property
    @abstractmethod
    def resources(self) -> Dict[str, Any]:
        """The resources that the task needs to run successfully on a given cluster queue."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def validate_and_create_concrete_resource(
        cls: Any, queue: ClusterQueue, requested_resources: Dict[str, Any]
    ) -> Tuple[bool, str, ConcreteResource]:
        """Validate that the resources are available on the queue and return an instance.

        Args:
            queue: The queue to validate the requested resources agains.
            requested_resources: Which resources the user wants to run the task with on the
                given queue.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def adjust_and_create_concrete_resource(
        cls: Any,
        expected_queue: ClusterQueue,
        existing_resources: Dict[str, Any],
        fallback_queues: Optional[List[ClusterQueue]],
        resource_scales: Optional[Dict[str, float]],
    ) -> ConcreteResource:
        """Adjust resources after a resource error is detected by the distributor.

        Args:
            expected_queue: The queue we expect to run on.
            existing_resources: The resources the user ran with previously that failed due to
                a resource error.
            fallback_queues: list of queues that users specify. If their jobs exceed the
                resources of a given queue, Jobmon will try to run their jobs on the fallback
                queues.
            resource_scales: Specifies how much to scale the failed Task's resources by.
        """
        raise NotImplementedError

    def __hash__(self) -> int:
        """Determine the hash of a concrete resources object."""
        # Note: this algorithm assumes all keys and values in the resources dict are
        # JSON-serializable. Since that's a requirement for logging in the database,
        # this assumption should be safe.

        # Uniqueness is determined by queue name and the resources parameter.
        hashval = hashlib.sha1()
        hashval.update(bytes(str(hash(self.queue.queue_name)).encode('utf-8')))
        hashval.update(bytes(str(
            hash(json.dumps(self.resources, sort_keys=True))).encode('utf-8')))
        return int(hashval.hexdigest(), 16)

    def __eq__(self, other: object) -> bool:
        """Check equality of task resources objects."""
        if not isinstance(other, ConcreteResource):
            return False
        return hash(self) == hash(other)