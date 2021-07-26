"""Cluster objects define where a user wants their tasks run. e.g. UGE, Azure, Seq."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.api import import_cluster, register_cluster_plugin
from jobmon.cluster_type.base import ClusterQueue, ConcreteResource
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeCluster, SerializeQueue


logger = logging.getLogger(__name__)


class Cluster:
    """Cluster objects define where a user wants their tasks run. e.g. UGE, Azure, Seq."""

    def __init__(self, cluster_name: str, requester: Optional[Requester] = None) -> None:
        """Initialization of Cluster."""
        self.cluster_name = cluster_name

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self.queues: Dict[str, ClusterQueue] = {}

    @classmethod
    def get_cluster(cls: Any, cluster_name: str, requester: Optional[Requester] = None) \
            -> Cluster:
        """Get a bound instance of a Cluster.

        Args:
            cluster_name: the name of the cluster
            requester (Requester): requester object to connect to Flask service.
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
    def plugin(self) -> Any:
        """If the cluster is bound, return the cluster interface for the type of cluster."""
        if not self.is_bound:
            raise AttributeError("Cannot access plugin until Cluster is bound to database")
        return import_cluster(self._cluster_type_name)

    @property
    def concrete_resource_class(self) -> type:
        """ If the cluster is bound, access the concrete resource class"""
        return self.plugin.get_concrete_resource_class()

    def get_queue(self, queue_name: str) -> ClusterQueue:
        """Get the ClusterQueue object associated with a given queue_name.

        Checks if queue object is in the cache, if it's not it will query the database and add
        the queue object to the cache.

        Args:
            queue_name: name of the queue you want.
        """
        # this is cached so should be fast
        try:
            queue = self.queues[queue_name]
        except KeyError:
            queue_class = self.plugin.get_cluster_queue_class()
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
            queue = queue_class(**queue_kwargs)
            self.queues[queue_name] = queue

        return queue

    def validate_requested_resources(self, requested_resources: Dict[str, Any],
                                     queue: ClusterQueue,
                                     fail: bool = False) -> ConcreteResource:
        """Validate the requested resources dict against the specified queue.

        Raises: ValueError
        """
        # validate it has required resources
        valid_resources: ConcreteResource = self.concrete_resource_class.validate(
            queue=queue, requested_resources=requested_resources, fail=fail)
        return valid_resources

    def adjust_task_resource(self, initial_resources: Dict, resource_scales: Dict,
                             expected_queue: ClusterQueue = None,
                             fallback_queues: List[ClusterQueue] = None) -> TaskResources:
        """Adjust task resources based on the scaling factor"""
        adjusted_resource: ConcreteResource = self.concrete_resource_class.adjust(
            existing_resources=initial_resources,
            resource_scales=resource_scales,
            expected_queue=expected_queue,
            fallback_queues=fallback_queues)
        return adjusted_resource

    def create_valid_task_resources(self, resource_params: Dict, task_resources_type_id: str,
                                    fail=False) -> TaskResources:
        """Construct a TaskResources object with the specified resource parameters.
        Validate before constructing task resources, taskResources assumed to be valid
        """
        queue_name = resource_params.pop("queue")
        queue = self.get_queue(queue_name)

        # Validate
        is_valid, msg, concrete_resources = self.concrete_resource_class.validate_and_create_concrete_resource(
            requested_resources=resource_params,
            queue=queue
        )
        if fail and not is_valid:
            raise ValueError(f"Failed validation, reasons: {msg}")

        task_resource = TaskResources(queue=concrete_resources.queue,
                                      concrete_resources=concrete_resources,
                                      task_resources_type_id=task_resources_type_id)
        return task_resource
