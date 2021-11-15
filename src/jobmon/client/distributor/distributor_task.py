"""Task object used by distributor to create Task Instances from."""
from __future__ import annotations

import logging
from typing import Type

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
        array_id: int,
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

    def register_task_instance(
        self,
        workflow_run_id: int,
    ) -> DistributorTaskInstance:
        """create a task instance associated with this task.

        Args:
            workflow_run_id (int): the workflow run id
            requester: requester for communicating with central services
        """
        app_route = "/task_instance"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "task_id": self.task_id,
                "array_id": self.array_id,
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

        return DistributorTaskInstance.from_wire(
            response["task_instance"],
            requester=self.requester,
        )
