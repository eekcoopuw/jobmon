from __future__ import annotations

import logging
import hashlib
from typing import Callable, Dict, List, Set, Tuple, TYPE_CHECKING, Union

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance


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
        self._distributor_id = None  # launch method should add distributor ID to this object

    @property
    def distributor_id(self) -> Union[str, int]:
        if self._distributor_id is None:
            raise AttributeError("Distributor ID cannot be accessed before the task instance "
                                 "is launched.")
        return self._distributor_id

    def add_batch_number_to_task_instances(self) -> int:
        """Add the current batch number to the current set of registered task instance ids."""
        app_route = f'/task_instance/record_array_batch_num/{self.batch_number}'
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                'task_instance_ids': [task_instance.task_instance_id for task_instance in
                                      self.task_instances],
            },
            request_type='post'
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )

    def get_task_resources(self):
        pass

    def launch_array_batch(self) -> Tuple[Set[DistributorTaskInstance], List[Callable]]:
        pass

    def get_queueing_errors(
        self, cluster: ClusterDistributor
    ) -> Tuple[Set, List[Callable]]:

        errors = cluster.get_array_queueing_errors(self.distributor_id)

        # Add work to terminate the eqw task instances, if any
        if len(errors) > 0:
            return set(), [DistributorCommand(self.terminate_task_instances, cluster, errors)]
        else:
            return set(), []

    def terminate_task_instances(
        self, cluster: ClusterDistributor, errors: Dict[str, str]
    ) -> Tuple[Set, List[Callable]]:

        commands = []
        task_instances = {ti.distributor_id: ti for ti in self.task_instances}
        for distributor_id, error_msg in errors.items():
            task_instance = task_instances[distributor_id]
            commands.append(DistributorCommand(task_instance.transition_to_error,
                                               error_msg, TaskInstanceStatus.UNKNOWN_ERROR))

        cluster.terminate_task_instances(list(errors.keys()))

        return set(), commands

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.array_id)).encode("utf-8"))
        hash_value.update(str(self.batch_number).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)
