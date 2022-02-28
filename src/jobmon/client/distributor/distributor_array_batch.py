from __future__ import annotations

import ast
import hashlib
import logging
from typing import Callable, Dict, List, Set, Tuple, TYPE_CHECKING, Union

from jobmon.client.distributor.distributor_command import DistributorCommand
from jobmon.cluster_type.base import ClusterDistributor
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
    def distributor_id(self) -> Union[str, int]:
        if self._distributor_id is None:
            raise AttributeError(
                "Distributor ID cannot be accessed before the task instance is launched."
            )
        return self._distributor_id

    @distributor_id.setter
    def distributor_id(self, val: int):
        self._distributor_id = val
        for task_instance in self.task_instances:
            task_instance.distributor_id = val

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

        task_resources_dict = SerializeTaskResources.kwargs_from_wire(response["task_resources"])
        self._requested_resources = ast.literal_eval(task_resources_dict["requested_resources"])

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

    def process_queueing_errors(
        self, cluster: ClusterDistributor, distributor_service: DistributorService
    ) -> None:
        assert distributor_service is not None


        errors = cluster.get_array_queueing_errors(self.distributor_id)

        # Add work to terminate the eqw task instances, if any
        if len(errors) > 0:
            command = DistributorCommand(self.terminate_task_instances, cluster, errors, distributor_service)
            distributor_service.distributor_commands.append(command)

    def terminate_task_instances(
        self, cluster: ClusterDistributor, errors: Dict[str, str], distributor_service: DistributorService
    ) -> None:
        """Terminate task instances with errors."""
        assert distributor_service is not None

        for task_instance in self.task_instances:
            for distributor_id, error_msg in errors.items():
                command = DistributorCommand(task_instance.transition_to_error,
                                             error_msg, TaskInstanceStatus.UNKNOWN_ERROR)
                distributor_service.distributor_commands.append(command)

        cluster.terminate_task_instances(list(errors.keys()))

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.array_id)).encode("utf-8"))
        hash_value.update(str(self.batch_number).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)
