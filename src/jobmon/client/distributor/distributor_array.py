"""Array object used by distributor to create arrays from."""
from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional, Set, Type, TYPE_CHECKING

from jobmon.constants import TaskInstanceStatus
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

        self.task_instances: Dict[int, DistributorTaskInstance] = {}
        self.current_batch_number = 0
        self.requester = requester

    def get_metadata(self):
        # app_route = f"/array/{array_id}"
        # return_code, response = self.requester.send_request(
        #     app_route=app_route, message={}, request_type="get", logger=logger
        # )
        # if http_request_ok(return_code) is False:
        #     raise InvalidResponse(
        #         f"Unexpected status code {return_code} from POST "
        #         f"request through route {app_route}. Expected "
        #         f"code 200. Response content: {response}"
        #     )
        pass

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

    @property
    def workflow_run(self) -> DistributorWorkflowRun:
        return self._distributor_workflow_run

    @workflow_run.setter
    def workflow_run(self, val: DistributorWorkflowRun):
        self._distributor_workflow_run = val

    @property
    def instantiated_task_instances(self) -> Set[DistributorTaskInstance]:
        task_instances = set(self.task_instances.values()).intersection(
            self.workflow_run.state_map[TaskInstanceStatus.INSTANTIATED]
        )
        return task_instances

    @property
    def launched_task_instances(self) -> Set[DistributorTaskInstance]:
        task_instances = set(self.task_instances.values()).intersection(
            self.workflow_run.state_map[TaskInstanceStatus.LAUNCHED]
        )
        return task_instances

    @property
    def running_task_instances(self) -> Set[DistributorTaskInstance]:
        task_instances = set(self.task_instances.values()).intersection(
            self.workflow_run.state_map[TaskInstanceStatus.RUNNING]
        )
        return task_instances

    @property
    def capacity(self) -> int:
        capacity = (
            self.max_concurrently_running
            - len(self.launched_task_instances)
            - len(self.running_task_instances)
        )
        return capacity

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.array_id != self.array_id:
            raise ValueError(
                f"array_id mismatch. TaskInstance={task_instance.array_id}. "
                f"Array={self.array_id}."
            )
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.array = self

    def get_task_instance_batch(
        self, task_resources_id: int, task_instances: Set[DistributorTaskInstance]
    ) -> Set[DistributorTaskInstance]:
        batch_eligable = self.instantiated_task_instances.intersection(task_instances)
        task_instance_batch = [
            task_instance for task_instance in batch_eligable
            if task_instance.task_resources_id == task_resources_id
        ]

    def add_batch_number_to_task_instances(
        self, task_instances: Iterable[DistributorTaskInstance]
    ) -> int:
        """Add the current batch number to the current set of registered task instance ids."""
        this_batch = self.batch_number
        app_route = f'/task_instance/record_array_batch_num/{self.batch_number}'
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                'task_instance_ids': self.instantiated_array_task_instance_ids,
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
        self.current_batch_number += 1
        return this_batch
