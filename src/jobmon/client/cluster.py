from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.api import register_cluster_plugin, import_cluster
from jobmon.cluster_type.base import ClusterQueue
from jobmon.constants import TaskResourcesType
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeCluster, SerializeQueue


logger = logging.getLogger(__name__)


class Cluster:

    def __init__(self, cluster_name: str, requester: Optional[Requester] = None) -> None:
        self.cluster_name = cluster_name

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.queues: Dict[str, ClusterQueue] = {}

    @classmethod
    def get_cluster(cls, cluster_name: str, requester: Optional[Requester] = None) -> Cluster:
        """Get a bound instance of a Cluster.

        Args:
            cluster_name: the name of the cluster
        """
        cluster = cls(cluster_name, requester)
        cluster.bind()
        return cluster

    def bind(self) -> None:
        """Bind Cluster to the database, getting an id back."""
        app_route = f'/client/cluster/{self.cluster_name}'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type="get",
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}'
            )
        cluster_kwargs = SerializeCluster.kwargs_from_wire(response["cluster"])

        self._cluster_id = cluster_kwargs["id"]
        self._cluster_type_name = cluster_kwargs["cluster_type_name"]
        register_cluster_plugin(self._cluster_type_name, cluster_kwargs["package_location"])

    @property
    def is_bound(self) -> bool:
        """If the Cluster has been bound to the database."""
        return hasattr(self, "_cluster_id")

    @property
    def id(self) -> int:
        """Unique id from database if Cluster has been bound."""
        if not self.is_bound:
            raise AttributeError("Cannot access id until Cluster is bound to database")
        return self._cluster_id

    @property
    def plugin(self):
        if not self.is_bound:
            raise AttributeError("Cannot access plugin until Cluster is bound to database")
        return import_cluster(self._cluster_type_name)

    def get_queue(self, queue_name: str) -> ClusterQueue:
        # this is cached so should be fast
        try:
            queue = self.queues[queue_name]
        except KeyError:
            Queue = self.plugin.get_cluster_queue_class()
            app_route = f'/client/cluster/{self.id}/queue/{queue_name}'
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={},
                request_type="get",
                logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected code '
                    f'200. Response content: {response}'
                )
            queue_kwargs = SerializeQueue.kwargs_from_wire(response["queue"])
            queue = Queue(**queue_kwargs)
            self.queues[queue_name] = queue

        return queue

    def _validate_requested_resources(self, requested_resources: Dict[str, Any],
                                      queue: ClusterQueue) -> None:
        """Validate the requested task resources against the specified queue.

        Raises: ValueError
        """
        # validate it has required resources
        full_error_msg = ""
        missing_resources = set(queue.required_resources) - set(requested_resources.keys())
        if missing_resources:
            full_error_msg = (
                f"\n  Missing required resources {list(missing_resources)} for "
                f"'{queue.queue_name}'. Got {list(requested_resources.keys)}."
            )

        for resource, resource_value in requested_resources.items():
            msg = queue.validate_resource(resource, resource_value, fail=False)
            full_error_msg += msg

        if full_error_msg:
            raise ValueError(full_error_msg)

    def adjust_task_resource(self, task_resources, adjustment_func):
        """Adjust task resources based on the scaling factor"""
        pass

    def create_task_resources(self, resource_params: Dict):
        resource_params = resource_params.get(self.cluster_name)
        queue_name = resource_params.pop("queue")
        queue = self.get_queue(queue_name)

        # construct resource scales
        try:
            resource_scales = resource_params.pop("resource_scales")
        except KeyError:
            resource_scales = {}

        try:
            self._validate_requested_resources(resource_params, queue)
        except ValueError as e:
            # TODO how to raise validation errors
            print(e)

        task_resource = TaskResources(queue_id=queue.queue_id,
                                      requested_resources=resource_params,
                                      resource_scales=resource_scales,
                                      task_resources_type_id=TaskResourcesType.VALIDATED)
        return task_resource
