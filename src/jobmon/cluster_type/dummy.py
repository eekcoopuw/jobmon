"""Dummy Executor fakes execution for testing purposes."""
from __future__ import annotations

import logging
import os
import random
from typing import Any, Dict, List, Optional, Tuple, Type

from jobmon.cluster_type.base import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
    ConcreteResource,
)
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.worker_node.cli import WorkerNodeCLI
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


logger = logging.getLogger(__name__)


class DummyQueue(ClusterQueue):
    """Implementation of the dummy executor queue, derived from ClusterQueue."""

    def __init__(self, queue_id: int, queue_name: str, parameters: dict) -> None:
        """Intialization of the the dummy processor queue.

        Get the limits from the database in the client.
        """
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resources(self, **kwargs: Dict) -> Tuple[bool, str, Dict]:
        """No resources defined for sequential execution. All resources valid."""
        return True, "", kwargs

    @property
    def queue_id(self) -> int:
        """Return the ID of the queue."""
        return self._queue_id

    @property
    def queue_name(self) -> str:
        """Return the name of the queue."""
        return self._queue_name

    @property
    def parameters(self) -> Dict:
        """Return the dictionary of parameters."""
        return self._parameters

    @property
    def required_resources(self) -> List:
        """No required resources for dummy executor, return empty list."""
        return []


class DummyDistributor(ClusterDistributor):
    """The Dummy Executor fakes the execution of a Task and acts as though it succeeded."""

    def __init__(self, *args: tuple, **kwargs: dict) -> None:
        """Initialization of the dummy distributor."""
        self.started = False
        # Parse the config
        worker_node_config = WorkerNodeConfig.from_defaults()
        self.heartbeat_report_by_buffer = worker_node_config.heartbeat_report_by_buffer
        self.task_instance_heartbeat_interval = (
            worker_node_config.task_instance_heartbeat_interval
        )

    @property
    def worker_node_entry_point(self) -> str:
        """Path to jobmon worker_node_entry_point."""
        return ""

    @property
    def cluster_type_name(self) -> str:
        """Return the name of the cluster type."""
        return "dummy"

    def start(self) -> None:
        """Start the executor."""
        self.started = True

    def stop(self, distributor_ids: List[int]) -> None:
        """Stop the executor."""
        self.started = False

    def get_queueing_errors(self, distributor_ids: List[int]) -> Dict[int, str]:
        """Dummy tasks never error, since they never run. So always return an empty dict."""
        return {}

    def get_submitted_or_running(self, distributor_ids: List[int]) -> List[int]:
        """Check which task instances are active."""
        raise NotImplementedError

    def terminate_task_instances(self, distributor_ids: List[int]) -> None:
        """No such thing as running Dummy tasks. Therefore, nothing to terminate."""
        raise NotImplementedError

    def submit_to_batch_distributor(
        self, command: str, name: str, requested_resources: Dict[str, Any], array_length: int = 0
    ) -> int:
        """Run a fake execution of the task.

        In a real executor, this is where qsub would happen. Here, since it's a dummy executor,
        we just get a random number.
        """
        logger.debug("This is the Dummy Distributor")
        # even number for non array tasks
        distributor_id = random.randint(1, int(1e6)) * 2
        os.environ["JOB_ID"] = str(distributor_id)

        cli = WorkerNodeCLI()
        args = cli.parse_args(command)

        # Bring in the worker node here since dummy executor is never run
        worker_node_task_instance = WorkerNodeTaskInstance(
            task_instance_id=args.task_instance_id,
            expected_jobmon_version=args.expected_jobmon_version,
            cluster_type_name=args.cluster_type_name,
        )

        # Log running, log done, and exit
        _, _, _ = worker_node_task_instance.log_running(
            self.heartbeat_report_by_buffer * self.task_instance_heartbeat_interval
        )
        worker_node_task_instance.log_done()

        return distributor_id

    def submit_array_to_batch_distributor(
        self, command: str, name: str, requested_resources: Dict[str, Any], array_length: int
    ) -> int:
        """Runs a fake execution of the task, exactly like regular submit to batch."""
        logger.debug("This is the Dummy Distributor")
        # odd number for array tasks
        distributor_id = random.randint(1, int(1e6)) * 2 + 1
        os.environ["JOB_ID"] = str(distributor_id)

        DummyWorkerNode.STEP_ID = 1

        for i in range(array_length):

            cli = WorkerNodeCLI()
            args = cli.parse_args(command)

            # Bring in the worker node here since dummy executor is never run
            worker_node_task_instance = WorkerNodeTaskInstance(
                array_id=args.array_id,
                batch_number=args.batch_number,
                expected_jobmon_version=args.expected_jobmon_version,
                cluster_type_name=args.cluster_type_name,
            )

            # Log running, log done, and exit
            _, _, _ = worker_node_task_instance.log_running(
                self.heartbeat_report_by_buffer * self.task_instance_heartbeat_interval
            )
            worker_node_task_instance.log_done()

        return distributor_id

    def get_remote_exit_info(self, distributor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable


class DummyWorkerNode(ClusterWorkerNode):
    """Get Executor Info for a Task Instance."""
    STEP_ID = 1
    def __init__(self) -> None:
        """Initialization of the dummy executor worker node."""
        self._distributor_id: Optional[int] = None
        self. _array_step_id = DummyWorkerNode.STEP_ID
        DummyWorkerNode.STEP_ID += 1

    @property
    def distributor_id(self) -> Optional[int]:
        """Executor id of the task."""
        if self._distributor_id is None:
            jid = os.environ.get("JOB_ID")
            if jid:
                self._distributor_id = int(jid)
        return self._distributor_id

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Exit info, error message."""
        return TaskInstanceStatus.ERROR, error_msg

    def get_usage_stats(self) -> Dict:
        """Usage information specific to the exector."""
        return {}

    @property
    def subtask_id(self) -> str:
        """Return distributor id for non array. Random for array."""
        if self._distributor_id % 2 == 0:
            return str(self._distributor_id)
        else:
            # this is not right
            return f"{self._distributor_id}.{random.randint(1, int(1e7))}"

    @property
    def array_step_id(self) -> int:
        return self._array_step_id


class ConcreteDummyResource(ConcreteResource):
    """A version of a private constructor in Python."""

    def __init__(self, queue: ClusterQueue, resources: Dict) -> None:
        """Always assumed to be valid.

        Don't call init directly, this object should be created by validate or adjust.
        """
        self._queue = queue
        self._resources = resources

    @property
    def queue(self) -> ClusterQueue:
        """The queue that the resources have been validated against."""
        return self._queue

    @property
    def resources(self) -> Dict[str, Any]:
        """The resources that the task needs to run successfully on a given cluster queue."""
        return self._resources

    @classmethod
    def validate_and_create_concrete_resource(
        cls: Any, queue: ClusterQueue, requested_resources: Dict
    ) -> Tuple[bool, str, ConcreteDummyResource]:
        """Validate that the resources are available on the queue and return an instance.

        Args:
            queue: The queue to validate the requested resources against.
            requested_resources: Which resources the user wants to run the task with on the
                given queue.
        """
        is_valid, msg, valid_resources = queue.validate_resources(**requested_resources)
        return is_valid, msg, cls(queue=queue, resources=valid_resources)

    @classmethod
    def adjust_and_create_concrete_resource(
        cls: Any,
        expected_queue: ClusterQueue,
        existing_resources: Dict,
        fallback_queues: Optional[List[ClusterQueue]],
        resource_scales: Optional[Dict[str, float]],
    ) -> ConcreteDummyResource:
        """No adjustment defined for dummy execution. Return original parameters."""
        return cls(queue=expected_queue, valid_resources=existing_resources)


def get_cluster_queue_class() -> Type[ClusterQueue]:
    """Return the queue class for the dummy executor."""
    return DummyQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    """Return the cluster distributor for the dummy executor."""
    return DummyDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    """Return the cluster worker node class for the dummy executor."""
    return DummyWorkerNode


def get_concrete_resource_class() -> Type[ConcreteResource]:
    """Return the queue class for the dummy executor."""
    return ConcreteDummyResource
