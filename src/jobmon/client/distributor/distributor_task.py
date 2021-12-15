"""Task object used by distributor to create Task Instances from."""
from __future__ import annotations

import logging
from typing import Optional, Type

from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeDistributorTask


logger = logging.getLogger(__name__)


class DistributorTask:
    """Task object used by distributor to create Task Instances from."""

    # this API should always match what's returned by
    # serializers.SerializeTask
    def __init__(
        self,
        task_id: int,
        array_id: Optional[int],
        name: str,
        command: str,
        requested_resources: dict,
        requester: Requester,
    ) -> None:
        """This is a Task object used on the client side when constructing task instances.

        Args:
            task_id: job_id associated with this task
            array_id: array_id associated with this task
            name: name associated with this task
            command: what command to run when executing
            requested_resources: the distributor resources for this task
            requester: requester for communicating with central services
        """
        self.task_id = task_id
        self.array_id = array_id
        self.name = name
        self.command = command
        self.requested_resources = requested_resources
        self.requester = requester

    @classmethod
    def from_wire(
        cls: Type[DistributorTask], wire_tuple: tuple, requester: Requester
    ) -> DistributorTask:
        """Construct instance from wire format the JQS gives.

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                format = serializers.DistributorTask.to_wire()
            requester (Requester, shared_requester): requester for
                communicating with central services.
        """
        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeDistributorTask.kwargs_from_wire(wire_tuple)

        # instantiate job
        logger.debug("Instantiate task tid {}".format(kwargs["task_id"]))
        executor_task = cls(
            task_id=kwargs["task_id"],
            array_id=kwargs["array_id"],
            name=kwargs["name"],
            command=kwargs["command"],
            requested_resources=kwargs["requested_resources"],
            requester=requester,
        )
        return executor_task

    def _get_array_batch_num(self) -> Optional[int]:
        """This is to determine which train the ti will board."""

        if self.array_id is None:
            return None
        else:
            # TODO: GBDSCI-4193
            return 1

    def register_task_instance(
        self,
        workflow_run_id: int
    ) -> DistributorTaskInstance:
        """create a task instance associated with this task.

        Args:
            workflow_run_id (int): the workflow run id
        """
        array_batch_num = self._get_array_batch_num()
        app_route = "/task_instance"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "task_id": self.task_id,
                "array_id": self.array_id,
                "array_batch_num": array_batch_num,
                "workflow_run_id": workflow_run_id,
            },
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        # get the cluster_type_id
        app_route = f"/cluster_type/task_id/{self.task_id}"
        return_code, resp = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type="get",
        )
        assert return_code == 200
        ctid_from_restful = resp["cluster_type_id"]

        # at this point cluster type can be None; replace it
        wire_tuple = response["task_instance"]
        wire_tuple[4] = ctid_from_restful

        distributor_ti = DistributorTaskInstance.from_wire(
            wire_tuple,
            requester=self.requester,
        )
        distributor_ti.name = self.name
        distributor_ti.requested_resources = self.requested_resources
        return distributor_ti


    def _showSelf(self):
        """This is a helper function to use in PDB."""
        print(f"task_id: {self.task_id} \n"
              f"array_id: {self.array_id} \n"
              f"array_batch_num: {self._get_array_batch_num()} \n")
