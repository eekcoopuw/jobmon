"""Array object used by distributor to create arrays from."""
from __future__ import annotations

import logging
from typing import Dict, List, Set, Type, TYPE_CHECKING

from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeDistributorArray

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import (
        DistributorTaskInstance,
    )

logger = logging.getLogger(__name__)


class DistributorArray:
    def __init__(self, array_id: int, requester: Requester):
        self.array_id = array_id

        self.task_instances: Set[DistributorTaskInstance] = set()
        self.last_batch_number = 0
        self.requester = requester
        self.max_concurrently_running = None

    def get_metadata(self):

        # refresh max_concurrently_running from db
        app_route = f"/array/{self.array_id}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        array_dict = SerializeDistributorArray.kwargs_from_wire(response["array"])
        self.max_concurrently_running = array_dict["max_concurrently_running"]

        # refresh last_batch_number from db
        app_route = f"/array/{self.array_id}/get_last_batch_number"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self.last_batch_number = \
            response["last_batch_number"] if response["last_batch_number"] is not None else 0

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
        array = cls(array_id=kwargs["array_id"], requester=requester)
        return array

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.array_id != self.array_id:
            raise ValueError(
                f"array_id mismatch. TaskInstance={task_instance.array_id}. "
                f"Array={self.array_id}."
            )
        self.task_instances.add(task_instance)
        task_instance.array = self

    def create_array_batches(
        self,
        eligible_task_instances: Set[DistributorTaskInstance]
    ) -> List[DistributorArrayBatch]:
        # TODO: would this logic make more sense in the SWARM???

        # limit eligible set to this array and store batches
        array_eligible = self.task_instances.intersection(eligible_task_instances)

        # return a list of commands to run
        array_batches: List[DistributorArrayBatch] = []

        # group into batches
        array_batch_sets: Dict[int, Set[DistributorTaskInstance]] = {}
        for task_instance in array_eligible:
            if task_instance.task_resources_id not in array_batch_sets:
                array_batch_sets[task_instance.task_resources_id] = set()
            array_batch_sets[task_instance.task_resources_id].add(task_instance)

        # construct the array batches
        for task_resources_id, batch_set in array_batch_sets.items():
            current_batch_number = self.last_batch_number + 1
            array_batch = DistributorArrayBatch(
                self.array_id,
                current_batch_number,
                task_resources_id,
                batch_set,
                self.requester,
            )
            for task_instance_in_batch in batch_set:
                task_instance_in_batch.array_batch = array_batch
            self.last_batch_number = current_batch_number
            array_batches.append(array_batch)

        return array_batches

    def __hash__(self):
        return self.array_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two arrays are equivalent."""
        if not isinstance(other, DistributorArray):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: DistributorArray) -> bool:
        """Check if one hash is less than the has of another DistributorArray."""
        return hash(self) < hash(other)
