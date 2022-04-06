from __future__ import annotations

import ast
import hashlib
import logging
from typing import Dict, Set, TYPE_CHECKING

from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeTaskResources

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import (
        DistributorTaskInstance,
    )


logger = logging.getLogger(__name__)


class DistributorArrayBatch:
    def __init__(
        self,
        array_id: int,
        batch_number: int,
        task_resources_id: int,
        task_instances: Set[DistributorTaskInstance],
        requester: Requester,
    ):
        self.array_id = array_id
        self.batch_number = batch_number
        self.task_resources_id = task_resources_id
        self.task_instances = task_instances

        self._requested_resources: Dict = {}

        self.requester = requester

        # TODO: array class should have a name in the client model GBDSCI-4184
        self.name = "foo"

    @property
    def requested_resources(self) -> Dict:
        if not self._requested_resources:
            raise AttributeError(
                "Requested Resources cannot be accessed before the array batch is prepared for"
                " launch."
            )
        return self._requested_resources

    def _load_requested_resources(self) -> None:
        app_route = f"/task_resources/{self.task_resources_id}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        task_resources = SerializeTaskResources.kwargs_from_wire(response["task_resources"])
        self._requested_resources = ast.literal_eval(task_resources["requested_resources"])

    def prepare_array_batch_for_launch(self) -> None:
        """Add the current batch number to the current set of registered task instance ids."""
        app_route = f"/task_instance/record_array_batch_num/{self.batch_number}"
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "task_instance_ids": [
                    task_instance.task_instance_id
                    for task_instance in self.task_instances
                ]
            },
            request_type="post",
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )
        array_step_id = 0
        for task_instance in sorted(self.task_instances):
            task_instance.array_step_id = array_step_id
            array_step_id += 1

        self._load_requested_resources()
        for task_instance in self.task_instances:
            task_instance.requested_resources = self.requested_resources

    def transition_to_launched(self, distributor_id_map: dict, next_report_by: float):
        """Transition all associated task instances to LAUNCHED state."""

        # Assertion that all bound task instances are indeed instantiated
        for ti in self.task_instances:
            if ti.status != TaskInstanceStatus.INSTANTIATED:
                raise ValueError(f"{ti} is not in INSTANTIATED state, prior to launching.")

        app_route = f'/array/{self.array_id}/log_distributor_id'
        data = {
            'batch_number': self.batch_number,
            'distributor_id_map': distributor_id_map,
            'next_report_increment': next_report_by
        }

        rc, resp = self.requester._send_request(
            app_route=app_route,
            message=data,
            request_type='post'
        )

        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )

        # Update status in memory for all task instances
        for ti in self.task_instances:
            ti.status = TaskInstanceStatus.LAUNCHED

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.array_id)).encode("utf-8"))
        hash_value.update(str(self.batch_number).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)
