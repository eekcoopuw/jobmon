"""Swarm side task object."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_resources import TaskResources
from jobmon.client.cluster import ClusterQueue
from jobmon.constants import TaskStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeSwarmTask


logger = logging.getLogger(__name__)


class SwarmTask(object):
    """Swarm side task object."""

    def __init__(self, task_id: int, status: str, task_args_hash: int,
                 # cluster: Cluster,
                 task_resources: Optional[TaskResources] = None,
                 resource_scales: Optional[Dict] = None,
                 max_attempts: int = 3,
                 fallback_queues: Optional[List[ClusterQueue]] = None,
                 requester: Optional[Requester] = None) -> None:
        """Implementing swarm behavior of tasks

        Args:
            task_id: id of task object from bound db object
            status: status of task object
            task_args_hash: hash of unique task arguments
            task_resources: callable to be executed when Task is ready to be run and
            resources can be assigned
            max_attempts: maximum number of task_instances before failure
            requester (Requester): Requester object to communicate with the flask services.
        """
        self.task_id = task_id
        self.status = status

        self.upstream_swarm_tasks: Set[SwarmTask] = set()
        self.downstream_swarm_tasks: Set[SwarmTask] = set()

        self.task_resources = task_resources

        # self.cluster = cluster
        # self.task_resources_callable = task_resources
        # self.resource_scales = resource_scales

        self.max_attempts = max_attempts
        self.task_args_hash = task_args_hash

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # once the callable is evaluated, the resources should be saved here
        # self.bound_parameters: List[TaskResources] = [self.task_resources_callable()]
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

    def queue_task(self) -> int:
        """Transition a task to the Queued for Instantiation status in the db."""
        rc, _ = self.requester.send_request(
            app_route=f'/swarm/task/{self.task_id}/queue',
            message={},
            request_type='post',
            logger=logger
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"{rc}: Could not queue task")
        self.status = TaskStatus.QUEUED_FOR_INSTANTIATION
        return rc

    def adjust_resources(self) -> TaskResources:
        """Function from Job Instance Factory that adjusts resources and then queues them.

        This should also incorporate resource binding if they have not yet been bound.
        """
        logger.debug("Job in A state, adjusting resources before queueing")
        # get the most recent parameter set
        previous_resources: TaskResources = self.bound_parameters[-1]

        new_resources: TaskResources = self.cluster.adjust_task_resource(
            initial_resources=previous_resources.concrete_resources.resources,
            resource_scales=self.resource_scales,
            expected_queue=previous_resources.queue,
            fallback_queues=self.fallback_queues)
        return new_resources
