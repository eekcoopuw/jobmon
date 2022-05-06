"""Inteface definition for jobmon executor plugins."""
from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# the following try-except is to accommodate Python versions on both >=3.8 and 3.7.
# The Protocol was officially introduced in 3.8, with typing_extensions slapped on 3.7.
try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable  # type: ignore

from jobmon import __version__
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.units import MemUnit, TimeUnit


class ClusterQueue(Protocol):
    """The protocol class for queues on a cluster."""

    @abstractmethod
    def __init__(self, queue_id: int, queue_name: str, parameters: Dict) -> None:
        """Initialization of ClusterQueue."""
        raise NotImplementedError

    @abstractmethod
    def validate_resources(self, fail: bool, **kwargs: Dict) -> Tuple[bool, str, Dict]:
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

    @staticmethod
    def convert_memory_to_gib(memory_str: str) -> int:
        """Given a memory request with a unit suffix, convert to GiB."""
        try:
            # User could pass in a raw value for memory, assume to be in GiB.
            # This is also the path taken by adjust
            return int(memory_str)
        except ValueError:
            return MemUnit.convert(memory_str, to="G")

    @staticmethod
    def convert_runtime_to_s(time_str: Union[str, float, int]) -> int:
        """Given a runtime request, coerce to seconds for recording in the DB."""

        try:
            # If a numeric is provided, assumed to be in seconds
            return int(time_str)
        except ValueError:

            time_str = str(time_str).lower()

            # convert to seconds if its datetime with a supported format
            try:
                time_object = datetime.strptime(time_str, "%H:%M:%S")
                time_seconds = (
                    time_object.hour * 60 * 60
                    + time_object.minute * 60
                    + time_object.second
                )
                time_str = str(time_seconds) + "s"
            except Exception:
                pass

            try:
                raw_value, unit = re.findall(r"[A-Za-z]+|\d+", time_str)
            except ValueError:
                # Raised if there are not exactly 2 values to unpack from above regex
                raise ValueError(
                    "The provided runtime request must be in a format of numbers "
                    "followed by one or two characters indicating the unit. "
                    "E.g. 1h, 60m, 3600s."
                )

            if "h" in unit:
                # Hours provided
                return TimeUnit.hour_to_sec(int(raw_value))
            elif "m" in unit:
                # Minutes provided
                return TimeUnit.min_to_sec(int(raw_value))
            elif "s" in unit:
                return int(raw_value)
            else:
                raise ValueError("Expected one of h, m, s as the suffixed unit.")


class ClusterDistributor(Protocol):
    """The protocol class for cluster distributors."""

    @property
    @abstractmethod
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_name(self) -> str:
        """Return the name of the cluster type."""
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start the distributor."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop the distributor."""
        raise NotImplementedError

    @abstractmethod
    def get_queueing_errors(self, distributor_ids: List[str]) -> Dict[str, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    @abstractmethod
    def get_submitted_or_running(
        self, distributor_ids: Optional[List[str]] = None
    ) -> Set[str]:
        """Check which task instances are active.

        Returns: a set strings
        """
        raise NotImplementedError

    @abstractmethod
    def terminate_task_instances(self, distributor_ids: List[str]) -> None:
        """Terminate task instances.

        If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def get_remote_exit_info(self, distributor_id: str) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

    @abstractmethod
    def submit_to_batch_distributor(
        self, command: str, name: str, requested_resources: Dict[str, Any]
    ) -> Tuple[str, str, str]:
        """Submit the command on the cluster technology and return a distributor_id.

        The distributor_id can be used to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If an exception is raised by
        this method the task instance will move to "W" state and the exception will be logged
        in the database under the task_instance_error_log table.

        Args:
            command: command to be run
            name: name of task
            requested_resources: resource requests sent to distributor API

        Returns:
            A tuple indicating the distributor id, the full output file location,
            and full error location.
        """
        raise NotImplementedError

    def submit_array_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources: Dict[str, Any],
        array_length: int,
    ) -> Dict[int, Tuple[str, str, str]]:
        """Submit an array task to the underlying distributor and return a distributor_id.

        The distributor ID represents the ID of the overall array job, sub-tasks will have
        their own associated IDs.

        Args:
            command: the array worker node command to run
            name: name of the array
            requested_resources: resources with which to run the array
            array_length: how many tasks associated with the array
        Return:
            a mapping of array_step_id to distributor_id, output location, and error location.
        """
        raise NotImplementedError

    def build_worker_node_command(
        self,
        task_instance_id: Optional[int] = None,
        array_id: Optional[int] = None,
        batch_number: Optional[int] = None,
    ) -> str:
        """Build a command that can be executed by the worker_node.

        Args:
            task_instance_id: id for the given instance of this task
            array_id: id for the array if using an array strategy
            batch_number: if array strategy is used, the submission counter index to use

        Returns:
            (str) unwrappable command
        """
        wrapped_cmd = ["worker_node"]
        if task_instance_id is not None:
            wrapped_cmd.extend(["--task_instance_id", str(task_instance_id)])
        if array_id is not None:
            wrapped_cmd.extend(["--array_id", str(array_id)])
        if batch_number is not None:
            wrapped_cmd.extend(["--batch_number", str(batch_number)])
        wrapped_cmd.extend(
            [
                "--expected_jobmon_version",
                __version__,
                "--cluster_name",
                self.cluster_name,
            ]
        )
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
    def distributor_id(self) -> Optional[str]:
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

    def array_step_id(self) -> Optional[int]:
        """The step id in each batch.

        For each array task instance, array_id, array_batch_num, and array_step_id
        should uniquely identify a subtask_id.

        It depends on the plug in whether you can generate the subtask_id using
        array_step_id.
        """
        raise NotImplementedError


@runtime_checkable
class ConcreteResource(Protocol):
    """The protocol class for concrete resources."""

    @abstractmethod
    def __init__(self, queue: ClusterQueue, requested_resources: Dict) -> None:
        """Initialization of ClusterQueue."""
        raise NotImplementedError

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
        hashval.update(bytes(str(hash(self.queue.queue_name)).encode("utf-8")))
        hashval.update(
            bytes(str(hash(json.dumps(self.resources, sort_keys=True))).encode("utf-8"))
        )
        return int(hashval.hexdigest(), 16)

    def __eq__(self, other: object) -> bool:
        """Check equality of task resources objects."""
        if not isinstance(other, ConcreteResource):
            return False
        return hash(self) == hash(other)

    def __repr__(self) -> str:
        """A representation string for a ConcreteResource instance."""
        return str({self.queue.queue_name: self.resources})
