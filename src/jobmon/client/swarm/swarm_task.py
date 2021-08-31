"""Swarm side task object."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.cluster import Cluster
from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.base import ClusterQueue
from jobmon.constants import TaskStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeSwarmTask


logger = logging.getLogger(__name__)


class SwarmTask(object):
    """Swarm side task object."""

    def __init__(self, task_id: int, task_hash: int, status: str, task_args_hash: int,
                 cluster: Cluster,
                 task_resources: Optional[TaskResources] = None,
                 resource_scales: Optional[Dict] = None,
                 max_attempts: int = 3,
                 fallback_queues: Optional[List[ClusterQueue]] = None,
                 requester: Optional[Requester] = None) -> None:
        """Implementing swarm behavior of tasks.

        Args:
            task_id: id of task object from bound db object.
            task_hash: hash(Task).
            status: status of task object.
            task_args_hash: hash of unique task arguments.
            cluster: The name of the cluster that the user wants to run their tasks on.
            task_resources: callable to be executed when Task is ready to be run and
                resources can be assigned.
            resource_scales: The rate at which a user wants to scale their requested resources
                after failure.
            max_attempts: maximum number of task_instances before failure.
            fallback_queues: A list of queues that users want to try if their original queue
                isn't able to handle their adjusted resources.
            requester: Requester object to communicate with the flask services.
        """
        self.task_id = task_id
        self.task_hash = task_hash
        self.status = status

        self.upstream_swarm_tasks: Set[SwarmTask] = set()
        self.downstream_swarm_tasks: Set[SwarmTask] = set()

        self.task_resources = task_resources

        self.resource_scales = resource_scales
        self.cluster = cluster

        self.max_attempts = max_attempts
        self.task_args_hash = task_args_hash

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.fallback_queues = fallback_queues

        self.num_upstreams_done: int = 0

    @staticmethod
    def from_wire(wire_tuple: tuple, swarm_tasks_dict: Dict[int, SwarmTask]) -> SwarmTask:
        """Return dict of swarm_task attributes from db."""
        kwargs = SerializeSwarmTask.kwargs_from_wire(wire_tuple)
        swarm_tasks_dict[kwargs["task_id"]].status = kwargs["status"]
        return swarm_tasks_dict[kwargs["task_id"]]

    def to_wire(self) -> tuple:
        """Send attributes to db."""
        return SerializeSwarmTask.to_wire(self.task_id, self.status)

    @property
    def all_upstreams_done(self) -> bool:
        """Return a bool of if upstreams are done or not."""
        if (self.num_upstreams_done >= len(self.upstream_tasks)):
            logger.debug(f"task id: {self.task_id} is checking all upstream tasks")
            return all([u.is_done for u in self.upstream_tasks])
        else:
            return False

    @property
    def is_done(self) -> bool:
        """Return a book of if this task is done or now."""
        return self.status == TaskStatus.DONE

    @property
    def downstream_tasks(self) -> List[SwarmTask]:
        """Return list of downstream tasks."""
        return list(self.downstream_swarm_tasks)

    @property
    def upstream_tasks(self) -> List[SwarmTask]:
        """Return a list of upstream tasks."""
        return list(self.upstream_swarm_tasks)

    def queue_task(self) -> None:
        """Transition a task to the Queued for Instantiation status in the db."""
        rc, _ = self.requester.send_request(
            app_route=f'/task/{self.task_id}/queue',
            message={},
            request_type='post',
            logger=logger
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"{rc}: Could not queue task")
        self.status = TaskStatus.QUEUED_FOR_INSTANTIATION

    def adjust_task_resources(self) -> None:
        """Adjust the swarm task's parameters.

        Use the cluster API to generate the new resources, then bind to input swarmtask.
        """
        if self.task_resources is None:
            raise RuntimeError("Cannot adjust resources until workflow is bound.")

        # current resources
        initial_resources = self.task_resources.concrete_resources.resources
        expected_queue = self.task_resources.queue

        # adjustment params
        resource_scales = self.resource_scales
        fallback_queues = self.fallback_queues

        new_task_resources = self.cluster.adjust_task_resource(
            initial_resources=initial_resources,
            resource_scales=resource_scales,
            expected_queue=expected_queue,
            fallback_queues=fallback_queues
        )
        new_task_resources.bind(task_id=self.task_id)
        self.task_resources = new_task_resources
