from __future__ import annotations

import hashlib
import logging
from typing import Callable, Dict, List, Set, Tuple, TYPE_CHECKING, Union

from jobmon.client.distributor.distributor_command import DistributorCommand
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

    def _record_array_batch_num(self) -> None:
        """Add the current batch number to the current set of registered task instance ids."""
        app_route = f'/task_instance/record_array_batch_num/{self.batch_number}'
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={'task_instance_ids': [task_instance.task_instance_id for task_instance
                                           in self.task_instances]},
            request_type='post'
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )

    def _load_requested_resources(self):
        if not self._requested_resources:
            # TODO: actually load them
            self._requested_resources = {}
        for task_instance in self.task_instances:
            task_instance.requested_resources = self._requested_resources

    def get_queueing_errors(
        self, cluster: ClusterDistributor
    ) -> Tuple[Set, List[DistributorCommand]]:

        errors = cluster.get_array_queueing_errors(self.distributor_id)

        # Add work to terminate the eqw task instances, if any
        if len(errors) > 0:
            return [DistributorCommand(self.terminate_task_instances, cluster, errors)]
        else:
            return []

    def launch(
        self,
        distributor: ClusterDistributor,
        next_report_increment: int
    ) -> List[DistributorCommand]:
        # record batch info in db
        self._record_array_batch_num()
        # get cluster specific launch info
        self._load_requested_resources()

        distributor_commands: List[DistributorCommand] = []

        # build worker node command
        command = distributor.build_worker_node_command(
            task_instance_id=None,
            array_id=self.array_id,
            batch_number=self.batch_number
        )
        try:
            # submit array to distributor
            self._distributor_id = distributor.submit_array_to_batch_distributor(
                command=command,
                name=self.name,
                requested_resources=self._requested_resources,
                array_length=len(self.task_instances)
            )

        except NotImplementedError:
            # create DistributorCommands to submit the launch if array isn't implemented
            for task_instance in self.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.launch,
                    distributor,
                    next_report_increment
                )
                distributor_commands.append(distributor_command)

        else:
            # create DistributorCommands to log the transition
            step_id = 0
            for task_instance in self.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.transition_to_launched,
                    self.distributor_id,
                    step_id,
                    next_report_increment
                )
                distributor_commands.append(distributor_command)
                step_id += 1

        return distributor_commands

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
