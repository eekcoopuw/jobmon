"""The client Task Resources with the resources initiation and binding to Task ID."""
from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Dict, List, Optional

from jobmon.client.client_config import ClientConfig
from jobmon.cluster_type.base import ClusterQueue, ConcreteResource
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester
import structlog as logging

logger = logging.getLogger(__name__)


class TaskResources:
    """An object representing the resources for a specific task."""

    def __init__(self, queue: ClusterQueue, task_resources_type_id: str,
                 concrete_resources: ConcreteResource, requester: Optional[Requester] = None) -> None:
        """Initialize the task resource object."""
        self._default_queue = queue
        self._task_resources_type_id = task_resources_type_id
        self._concrete_resources = concrete_resources

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self._requester = requester

    def __call__(self) -> TaskResources:
        """Return TaskResource object."""
        return self

    @property
    def is_bound(self) -> bool:
        """If the TaskResources has been bound to the database."""
        return hasattr(self, "_id")

    @property
    def id(self) -> int:
        """If the task resources has been bound to the database."""
        if not self.is_bound:
            raise AttributeError("Cannot access id until TaskResources is bound to database")
        return self._id

    @property
    def task_id(self) -> int:
        """If the task resources has been bound to the database."""
        if not self.is_bound:
            raise AttributeError(
                "Cannot access task_id until TaskResources is bound to database"
            )
        return self._task_id

    @property
    def queue_id(self) -> int:
        """Return the ID of the queue."""
        return self._queue_id

    @property
    def task_resources_type_id(self) -> str:
        """Return the type ID of the task resource."""
        return self._task_resources_type_id

    @property
    def concrete_resources(self) -> Dict:
        """Return the requested resources dictionary."""
        return self._concrete_resources

    @property
    def requester(self) -> Requester:
        """Return the requester."""
        return self._requester

    def bind(self, task_id: int) -> None:
        """Bind TaskResources to the database."""
        app_route = f'/swarm/task/{task_id}/bind_resources'
        msg = {
            "queue_id": self._queue_id,
            "task_resources_type_id": self._task_resources_type_id,
            "resource_scales": self._resource_scales,
            "requested_resources": self._requested_resources,
        }
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=msg,
            request_type='post',
            logger=logger
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        self._id = response
        self._task_id = task_id

    def to_wire(self) -> Dict:
        """Resources to dictionary."""
        return {
            "queue_id": self._queue_id,
            "task_resources_type_id": self._task_resources_type_id,
            "resource_scales": self._resource_scales,
            "requested_resources": self._requested_resources,
        }
