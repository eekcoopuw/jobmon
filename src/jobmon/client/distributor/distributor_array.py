"""Array object used by distributor to create arrays from."""
from __future__ import annotations

import logging
from typing import Dict, List, Type

from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeDistributorArray


logger = logging.getLogger(__name__)


class DistributorArray:

    def __init__(
        self,
        array_id: int,
        task_resources_id: int,
        requested_resources: Dict,
        requester: Requester
    ):
        self.array_id = array_id
        self.task_resources_id = task_resources_id
        self.requested_resources = requested_resources
        self.registered_array_task_instance_ids: List[int] = []

    @classmethod
    def from_wire(
        cls: Type[DistributorArray], wire_tuple: tuple, requester: Requester
    ) -> DistributorArray:
        """Construct instance from wire format the server gives.

        Args:
            wire_tuple: tuple representing the wire format for this task.
                format = serializers.DistributorArray.to_wire()
            requester: requester for communicating with central services.
        """
        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeDistributorArray.kwargs_from_wire(wire_tuple)

        # instantiate job
        array = cls(
            array_id=kwargs["array_id"],
            task_resources_id=kwargs["task_resources_id"],
            requested_resources=kwargs["requested_resources"],
            requester=requester,
        )
        return array

    def queue_task_instance_id_for_array_launch(self, task_instance_id: int):
        """
        Add task instance to array queue
        """
        self.registered_array_task_instance_ids.append(task_instance_id)

    def clear_registered_task_registry(self) -> None:
        """Clear all registered tasks that have already been submitted.

        Called when the array is submitted to the batch distributor."""
        # TODO: Safe for sequential, may have problems with async and centeralized distributor
        self.registered_array_task_instance_ids = []
