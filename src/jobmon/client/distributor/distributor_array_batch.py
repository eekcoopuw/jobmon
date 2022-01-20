from __future__ import annotations

import logging
from typing import Callable, Dict, List, Set, Tuple, TYPE_CHECKING

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_service import DistributorService


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

        # TODO: array class should have a name in the client model GBDSCI-4184
        self.name = "foo"

    def _record_array_batch_num(self, ids_to_launch: List[int]) -> int:
        """Add the current batch number to the current set of registered task instance ids."""
        app_route = f'/task_instance/record_array_batch_num/{self.batch_number}'
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={'task_instance_ids': ids_to_launch},
            request_type='post'
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )

    def _get_requested_resources(self) -> Dict:
        pass

    def launch_array_batch(
        self,
        distributor: ClusterDistributor
    ) -> Tuple[Set[DistributorTaskInstance], List[Callable]]:
        # record batch info in db
        ids_to_launch = [task_instance.task_instance_id for task_instance
                         in self.task_instances]
        self._record_array_batch_num(ids_to_launch)

        # get cluster specific launch info
        requested_resources = self._get_requested_resources()

        # build worker node command
        command = distributor.build_worker_node_command(
            task_instance_id=None,
            array_id=self.array_id,
            batch_number=self.batch_number
        )

        # submit it
        distributor_id = distributor.submit_array_to_batch_distributor(
            command=command,
            name=self.name,
            requested_resources=requested_resources,
            array_length=len(ids_to_launch)
        )

        distributor_commands: List[DistributorCommand] = []
        for task_instance in self.task_instances:
            distributor_command = DistributorCommand(task_instance.transition, distributor_id,
                                                     step_id)
            distributor_commands.append(distributor_command)

        return set(), distributor_commands
