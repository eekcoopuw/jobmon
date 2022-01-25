"""Array object used by distributor to create arrays from."""
from __future__ import annotations

import logging
from typing import Dict, List, Set, Tuple, Type, TYPE_CHECKING

from jobmon.client.distributor.distributor_command import DistributorCommand
from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeDistributorArray

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun

logger = logging.getLogger(__name__)


class DistributorArray:

    def __init__(
        self,
        array_id: int,
        requester: Requester
    ):
        self.array_id = array_id

        self.task_instances: Set[DistributorTaskInstance] = set()
        self.array_batches: Set[DistributorArrayBatch] = set()
        self.last_batch_number = 0
        self.requester = requester

    def get_metadata(self):
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
        self.max_concurrently_running = 100

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
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.array = self

    def create_array_batches(
        self,
        eligable_task_instances: Set[DistributorTaskInstance]
    ) -> DistributorArrayBatch:
        # TODO: would this logic make more sense in the SWARM???

        # limit eligable set to this array and store batches
        array_eligable = self.task_instances.intersection(eligable_task_instances)

        # return a list of commands to run
        array_batches: Set[DistributorArrayBatch] = []

        # group into batches
        array_batch_sets: Dict[int, Set[DistributorTaskInstance]] = {}
        for task_instance in array_eligable:
            if task_instance.task_resources_id not in array_batch_sets:
                array_batch_sets[task_instance.task_resources_id] = set()
            array_batch_sets[task_instance.task_resources_id].add(task_instance)

        # construct the array batches
        for task_resources_id, batch_set in array_batch_sets.items():
            current_batch_number = self.last_batch_number + 1
            array_batch = DistributorArrayBatch(
                self.array_id, current_batch_number, task_resources_id, batch_set
            )
            self.last_batch_number = current_batch_number
            array_batches.add(array_batch)

        self.array_batches.update(array_batches)
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
