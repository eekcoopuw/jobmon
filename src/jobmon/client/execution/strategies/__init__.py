from typing import Protocol, Optional, Callable, Union, List, Dict
from abc import abstractmethod, abstractproperty

class ComputeResources(Protocol):

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
	def scale_resources(self, resource: str):
		raise NotImplementedError


class Queue(Protocol):

	def validate(self, resource: str, value: Union[int, float, str]):
		raise NotImplementedError


class Executor(Protocol):

    def distributor_start(self) -> None:
        """Start the executor."""
        raise NotImplementedError

    def distributor_stop(self, executor_ids: List[int]) -> None:
        """Stop the executor."""
        raise NotImplementedError

    def distributor_execute(self, command: str, name: str,
                            task_resources: ExecutorParameters) -> int:
        """Executor the command on the cluster technology

        Optionally, return an (int) executor_id which the subclass could
        use at a later time to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If the
        subclass does not intend to offer those functionalities, this method
        can return None.

        Args:
            command: command to be run
            name: name of task
            executor_parameters: executor specific requested resources
            executor_ids: running executor task ids already being tracked
        """
        raise NotImplementedError

    def distributor_get_queueing_errors(self, executor_ids: List[int]) -> Dict[int, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    def distributor_get_actual_submitted_or_running(self, executor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    def distributor_terminate_task_instances(self, executor_ids: List[int]) -> None:
        """If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    def distributor_get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable