from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING, Union

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import http_request_ok, Requester


if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.distributor.status_processor import StatusProcessor


logger = logging.getLogger(__name__)


class DistributorTask(StatusProcessor):

    def __init__(self, task_id: int, requester: Requester):
        self.task_id = task_id
        self.task_instances: Dict[int, DistributorTaskInstance] = {}
        self.requester = requester

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.task_id != self.task_id:
            raise ValueError(
                f"task_id mismatch. TaskInstance={task_instance.task_id}. "
                f"Task={self.task_id}."
            )
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.task = self

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

    @property
    def processed_task_instances(self) -> Set[DistributorTaskInstance]:
        processed_task_instances = self._processed_task_instances
        self._processed_task_instances = set()
        return processed_task_instances

    @property
    def new_work(self) -> Dict[str, List[Callable[DistributorService, None]]]:
        new_work = self._new_work
        self._new_work = {}
        return new_work

    def launch_task_instance(self, distributor_service: DistributorService) -> None:
        """
        submits a task instance on a given distributor.
        adds the new task instance to self.submitted_or_running_task_instances
        """
        # Fetch the worker node command
        command = cluster.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )
        # Submit to batch distributor
        distributor_id = cluster.submit_to_batch_distributor(
            command=command,
            name=task_instance.name,
            requested_resources=task_instance.requested_resources
        )

        # move from register queue to launch queue
        self._launched_task_instance_ids.add(task_instance.task_instance_id)
        self._registered_task_instance_ids.pop(task_instance.task_instance_id)
        resp = self.transition_task_instance(array_id=None,
                                             task_instance_ids=[task_instance.task_instance_id],
                                             distributor_id=distributor_id,
                                             status=TaskInstanceStatus.LAUNCHED)

        # Pull unsuccessful transitions from the response, and add to a triaging queue
        erroneous_tis: Dict[int, str] = resp['erroneous_transitions']
        if len(erroneous_tis) > 0:
            self._move_to_the_right_queue(task_instance.task_instance_id,
                                          erroneous_tis[str(task_instance.task_instance_id)])

        # Return ti_distributor_id
        return distributor_id
