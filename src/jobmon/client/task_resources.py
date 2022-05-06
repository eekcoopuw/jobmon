"""The client Task Resources with the resources initiation and binding to Task ID."""
from __future__ import annotations

from http import HTTPStatus as StatusCodes
import logging
from typing import Dict, Optional

from jobmon.client.client_config import ClientConfig
from jobmon.cluster_type import ClusterQueue, ConcreteResource
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class TaskResources:
    """An object representing the resources for a specific task."""

    def __init__(
        self,
        task_resources_type_id: str,
        concrete_resources: ConcreteResource,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialize the task resource object."""
        self._task_resources_type_id = task_resources_type_id
        self._concrete_resources = concrete_resources

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self._requester = requester
        self._requested_resources = concrete_resources.resources

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
            raise AttributeError(
                "Cannot access id until TaskResources is bound to database"
            )
        return self._id

    @property
    def queue(self) -> ClusterQueue:
        """Return the queue."""
        return self._concrete_resources.queue

    @property
    def task_resources_type_id(self) -> str:
        """Return the type ID of the task resource."""
        return self._task_resources_type_id

    @property
    def concrete_resources(self) -> ConcreteResource:
        """Return the requested resources dictionary."""
        return self._concrete_resources

    @property
    def requester(self) -> Requester:
        """Return the requester."""
        return self._requester

    def bind(self, task_resources_type_id: str = None) -> None:
        """Bind TaskResources to the database."""
        # Check if it's already been bound
        if self.is_bound:
            logger.debug(
                "This task resource has already been bound, and assigned"
                f"task_resources_id {self.id}"
            )
            return

        app_route = "/task/bind_resources"
        if task_resources_type_id is None:
            task_resources_type_id = self._task_resources_type_id
        msg = {
            "queue_id": self.queue.queue_id,
            "task_resources_type_id": task_resources_type_id,
            "requested_resources": self._requested_resources,
        }
        return_code, response = self.requester.send_request(
            app_route=app_route, message=msg, request_type="post"
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._id = response

    def to_wire(self) -> Dict:
        """Resources to dictionary."""
        return {
            "queue_id": self.queue.queue_id,
            "task_resources_type_id": self._task_resources_type_id,
            "requested_resources": self._requested_resources,
        }

    def __hash__(self) -> int:
        """Determine the hash of a task resources object."""
        return hash(self.concrete_resources)

    def __eq__(self, other: object) -> bool:
        """Check equality of task resources objects."""
        if not isinstance(other, TaskResources):
            return False
        return hash(self) == hash(other)

    def __repr__(self) -> str:
        """A representation string for a TaskResources instance."""
        repr_string = (
            f"TaskResources(task_resources_type_id={self.task_resources_type_id}, "
            f"concrete_resources={self.concrete_resources}"
        )

        try:
            repr_string += f", id={self.id})"
        except AttributeError:
            repr_string += ")"
        return repr_string
