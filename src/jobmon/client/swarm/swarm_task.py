"""Swarm side task object."""
from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_resources import TaskResources
from jobmon.cluster import Cluster
from jobmon.cluster_type.base import ClusterQueue
from jobmon.constants import TaskStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeSwarmTask


logger = logging.getLogger(__name__)


class SwarmTask(object):
    """Swarm side task object."""

    def __init__(
        self,
        task_id: int,
        status: str,
        max_attempts: int,
        task_resources: TaskResources,
        cluster: Cluster,
        resource_scales: Optional[Dict] = None,
        fallback_queues: Optional[List[ClusterQueue]] = None,
        compute_resources_callable: Optional[Callable] = None,
        requester: Optional[Requester] = None,
    ) -> None:
        """Implementing swarm behavior of tasks.

        Args:
            task_id: id of task object from bound db object.
            status: status of task object.
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
        self.status = status

        self.upstream_swarm_tasks: Set[SwarmTask] = set()
        self.downstream_swarm_tasks: Set[SwarmTask] = set()

        self.task_resources = task_resources
        self.compute_resources_callable = compute_resources_callable
        self.fallback_queues = fallback_queues
        self.resource_scales = resource_scales if resource_scales is not None else {}
        self.cluster = cluster

        self.max_attempts = max_attempts
        self.num_upstreams_done: int = 0

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    @property
    def all_upstreams_done(self) -> bool:
        """Return a bool of if upstreams are done or not."""
        if self.num_upstreams_done >= len(self.upstream_tasks):
            logger.debug(f"task id: {self.task_id} is checking all upstream tasks")
            return all([u.status == TaskStatus.DONE for u in self.upstream_tasks])
        else:
            return False

    @property
    def downstream_tasks(self) -> List[SwarmTask]:
        """Return list of downstream tasks."""
        return list(self.downstream_swarm_tasks)

    @property
    def upstream_tasks(self) -> List[SwarmTask]:
        """Return a list of upstream tasks."""
        return list(self.upstream_swarm_tasks)

    @property
    def task_resources(self) -> TaskResources:
        """Get the id of the task if it has been bound to the db otherwise raise an error."""
        if not hasattr(self, "_task_resources"):
            raise AttributeError(
                "task_resources cannot be accessed before workflow is bound"
            )
        return self._task_resources

    @task_resources.setter
    def task_resources(self, val: TaskResources) -> None:
        if not isinstance(val, TaskResources):
            raise ValueError("task_resources must be of type=TaskResources")
        self._task_resources = val

    def queue_task(self, workflow_run_id: int) -> None:
        """Transition a task to the Queued for Instantiation status in the db."""
        rc, _ = self.requester.send_request(
            app_route=f"/task/{self.task_id}/queue",
            message={
                "workflow_run_id": workflow_run_id,
                "cluster_id": self.cluster.id,
                "task_resources_id": self.task_resources.id,
            },
            request_type="post",
            logger=logger,
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"{rc}: Could not queue task")
        self.status = TaskStatus.QUEUED

    def __hash__(self):
        return self.task_instance_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, SwarmTask):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: SwarmTask) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)
