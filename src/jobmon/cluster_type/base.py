"""Inteface definition for jobmon executor plugins."""

from abc import abstractmethod, abstractproperty
from typing import Callable, Dict, List, Optional, Protocol, Tuple, Union

from jobmon import __version__
from jobmon.exceptions import RemoteExitInfoNotAvailable


class ClusterResources(Protocol):

    # Plugins may or may not be subclassed
    # abcs are set here to distinguish properties and methods

    @abstractproperty
    def memory(self):
        raise NotImplementedError

    @abstractproperty
    def runtime(self):
        raise NotImplementedError

    @abstractproperty
    def cores(self):
        raise NotImplementedError

    @abstractproperty
    def queue(self):
        raise NotImplementedError

    @abstractmethod
    def adjust(self, adjust_func: Optional[Callable], **kwargs):
        raise NotImplementedError

    @abstractmethod
    def scale_resource(self, resource: str):
        raise NotImplementedError


class ClusterQueue(Protocol):

    @abstractmethod
    def __init__(self, queue_id: int, queue_name: str, parameters: Dict):
        raise NotImplementedError

    @abstractmethod
    def validate(self, resource: str, value: Union[int, float, str]) -> ClusterResources:
        raise NotImplementedError


class ClusterDistributor(Protocol):

    @abstractproperty
    def worker_node_wrapper_executable(self):
        """Path to jobmon worker node executable"""
        raise NotImplementedError

    @abstractproperty
    def cluter_type_name(self):
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start the executor."""
        raise NotImplementedError

    @abstractmethod
    def stop(self, executor_ids: List[int]) -> None:
        """Stop the executor."""
        raise NotImplementedError

    @abstractmethod
    def execute(self, command: str, name: str, compute_resources: ClusterResources) -> int:
        """Executor the command on the cluster technology

        Optionally, return an (int) executor_id which the subclass could
        use at a later time to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If the
        subclass does not intend to offer those functionalities, this method
        can return None.

        Args:
            command: command to be run
            name: name of task

        """
        raise NotImplementedError

    @abstractmethod
    def get_queueing_errors(self, executor_ids: List[int]) -> Dict[int, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    @abstractmethod
    def get_submitted_or_running(self, executor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    @abstractmethod
    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        """If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable

    def build_wrapped_command(self, task_instance_id: int,
                              heartbeat_interval: int, report_by_buffer: float
                              ) -> str:
        """Build a command that can be executed by the shell and can be unwrapped by jobmon
        itself to setup proper communication channels to the monitor server.
        Args:
            command: command to run the desired job
            task_instance_id: id for the given instance of this task

        Returns:
            (str) unwrappable command
        """
        wrapped_cmd = [
            "--task_instance_id", task_instance_id,
            "--expected_jobmon_version", __version__,
            "--executor_class", self.__class__.__name__,
            "--heartbeat_interval", heartbeat_interval,
            "--report_by_buffer", report_by_buffer
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

    @abstractproperty
    def executor_id(self) -> Optional[int]:
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
