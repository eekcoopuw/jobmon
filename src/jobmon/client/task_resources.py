"""The client Task Resources with the resources initiation and binding to Task ID."""
from http import HTTPStatus as StatusCodes
from typing import Optional
from jobmon.client.client_config import ClientConfig
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester

import structlog as logging

logger = logging.getLogger(__name__)

class TaskResources:

    def __init__(self, queue_id: int, task_resources_type_id: str, resource_scales: str,
                 requested_resources: str, requester: Optional[Requester] = None) -> None:

        self._task_id = None

        self._queue_id = queue_id
        self._task_resources_type_id = task_resources_type_id
        self._resource_scales = resource_scales
        self._requested_resources = requested_resources

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self._requester = requester

    @property
    def is_bound(self) -> bool:
        """If the TaskResources has been bound to the database."""
        return self._task_id is not None

    @property
    def task_id(self) -> int:
        """If the task resources has been bound to the database."""
        if not self.is_bound:
            raise AttributeError("Cannot access task_id until TaskResources is bound to database")
        return self._task_id

    @property
    def queue_id(self) -> int:
        return self._queue_id

    @property
    def task_resources_type_id(self) -> str:
        return self._task_resources_type_id

    @property
    def resource_scales(self) -> str:
        return self._resource_scales

    @property
    def requested_resources(self) -> str:
        return self._requested_resources

    @property
    def requester(self) -> Requester:
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

        self._task_id = task_id
