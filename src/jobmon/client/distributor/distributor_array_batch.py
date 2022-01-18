from __future__ import annotations

import logging
from typing import Callable, List, Set, Tuple, TYPE_CHECKING

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

    def get_queueing_errors(self, cluster: ClusterDistributor):
        errors = cluster.get_queueing_errors(self.array_id, self.batch_number)

        # Add work to terminate the eqw task instances
        commands = [DistributorCommand(self.terminate_task_instances, cluster, errors)]

        return set(), commands

    def terminate_task_instances(self, cluster: ClusterDistributor, errors: Dict[int, str]):

        commands = []
        for distributor_id, error_msg in errors.items():
            # TODO: Need to lookup task instance ID by distributor ID
            task_instance = self.task_instances.find(distributor_id)
            commands.append(DistributorCommand(task_instance.transition_to_unknown_error,
                                               error_msg))

        cluster.terminate_task_instances(list(errors.keys()))

        return set(), commands
