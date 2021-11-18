"""Array object used by distributor to create arrays from."""
from __future__ import annotations

import logging
from typing import Dict, List, Type

<<<<<<< HEAD
from jobmon.exceptions import InvalidResponse
=======
>>>>>>> 92dfb4d356e295eda0d3a789db1309705e05dfd8
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
<<<<<<< HEAD
        self.batch_number = 0
        self.requester = requester
=======
>>>>>>> 92dfb4d356e295eda0d3a789db1309705e05dfd8

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

    def add_batch_number_to_task_instances(self) -> None:
        """Add the current batch number to the current set of registered task instance ids."""
        app_route = f'/task_instance/record_array_batch_num/{self.batch_number}'
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                'task_instance_ids': self.registered_array_task_instance_ids,
            },
            request_type='post'
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )

        # Increment the counter for the next set of jobs
        self.batch_number += 1
