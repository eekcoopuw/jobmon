"""Task Instance object from the distributor's perspective."""
from __future__ import annotations

import logging
import time
from typing import Optional, Type

from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeTaskInstance


logger = logging.getLogger(__name__)


class DistributorTaskInstance:
    """Object used for communicating with JSM from the distributor node."""

    def __init__(
        self,
        task_instance_id: int,
        workflow_run_id: int,
        requester: Requester,
        requested_resources: Optional[dict] = None,
        name: Optional[str] = None,
        distributor_id: Optional[int] = None
    ) -> None:
        """Initialization of distributor task instance.

        Args:
            task_instance_id (int): a task_instance_id
            workflow_run_id (int): a workflow_run_id
            distributor_id (int, optional): the distributor_id associated with this
                task_instance
            requester (Requester, optional): a requester to communicate with
                the JSM. default is shared requester
        """
        self.task_instance_id = task_instance_id
        self.workflow_run_id = workflow_run_id
        self.distributor_id = distributor_id
        self.name = name
        self.requested_resources = requested_resources

        self.report_by_date: float

        self.error_state = ""
        self.error_msg = ""

        self.cluster_type_id: Optional[int] = None

        self.requester = requester

    @classmethod
    def from_wire(
        cls: Type[DistributorTaskInstance], wire_tuple: tuple, requester: Requester
    ) -> DistributorTaskInstance:
        """Create an instance from json that the JQS returns.

        Args:
            wire_tuple: tuple representing the wire format for this
                task. format = serializers.SerializeTask.to_wire()
            requester: requester for communicating with central services

        Returns:
            DistributorTaskInstance
        """
        kwargs = SerializeTaskInstance.kwargs_from_wire(wire_tuple)
        ti = cls(
            task_instance_id=kwargs["task_instance_id"],
            workflow_run_id=kwargs["workflow_run_id"],
            distributor_id=kwargs["distributor_id"],
            requester=requester,
        )
        return ti

    # def register_no_distributor_id(self, no_id_err_msg: str) -> None:
    #     """Register that submission failed with the central service.

    #     Args:
    #         no_id_err_msg: The error msg from the executor when failed to obtain distributor
    #             id.
    #     """
    #     app_route = f"/task_instance/{self.task_instance_id}/log_no_distributor_id"
    #     return_code, response = self.requester.send_request(
    #         app_route=app_route,
    #         message={"no_id_err_msg": no_id_err_msg},
    #         request_type="post",
    #         logger=logger,
    #     )
    #     if http_request_ok(return_code) is False:
    #         raise InvalidResponse(
    #             f"Unexpected status code {return_code} from POST "
    #             f"request through route {app_route}. Expected "
    #             f"code 200. Response content: {response}"
    #         )

    # def register_submission_to_batch_distributor(
    #     self, distributor_id: int, next_report_increment: float
    # ) -> None:
    #     """Register the submission of a new task instance to batch distributor.

    #     Args:
    #         distributor_id (int): distributor id created by distributor for this task
    #             instance
    #         next_report_increment: how many seconds to wait for
    #             report or status update before considering the task lost
    #     """
    #     self.distributor_id = distributor_id

    #     app_route = f"/task_instance/{self.task_instance_id}/log_distributor_id"
    #     return_code, response = self.requester.send_request(
    #         app_route=app_route,
    #         message={
    #             "distributor_id": str(distributor_id),
    #             "next_report_increment": next_report_increment,
    #         },
    #         request_type="post",
    #         logger=logger,
    #     )
    #     if http_request_ok(return_code) is False:
    #         raise InvalidResponse(
    #             f"Unexpected status code {return_code} from POST "
    #             f"request through route {app_route}. Expected "
    #             f"code 200. Response content: {response}"
    #         )

    #     self.report_by_date = time.time() + next_report_increment

    # def log_error(self) -> None:
    #     """Log an error from the distributor loops."""
    #     if self.distributor_id is None:
    #         raise ValueError("distributor_id cannot be None during log_error")
    #     distributor_id = self.distributor_id
    #     logger.debug(f"log_error for distributor_id {distributor_id}")
    #     if not self.error_state:
    #         raise ValueError("cannot log error if self.error_state isn't set")

    #     if self.error_state == TaskInstanceStatus.UNKNOWN_ERROR:
    #         app_route = f"/task_instance/{self.task_instance_id}/log_unknown_error"
    #     else:
    #         app_route = f"/task_instance/{self.task_instance_id}/log_known_error"

    #     return_code, response = self.requester.send_request(
    #         app_route=app_route,
    #         message={
    #             "error_state": self.error_state,
    #             "error_message": self.error_msg,
    #             "distributor_id": distributor_id,
    #         },
    #         request_type="post",
    #         logger=logger,
    #     )
    #     if http_request_ok(return_code) is False:
    #         raise InvalidResponse(
    #             f"Unexpected status code {return_code} from POST "
    #             f"request through route {app_route}. Expected "
    #             f"code 200. Response content: {response}"
    #         )

    def transition_to_launched(
        self,
        cluster_id: int,
        distributor_id: int,
        next_report_increment: float,
    ) -> None:
        """Register the submission of a new task instance to a cluster."""
        self.distributor_id = distributor_id

        app_route = f"/task_instance/{self.task_instance_id}/log_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "distributor_id": str(distributor_id),
                "next_report_increment": next_report_increment,
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

        self.report_by_date = time.time() + next_report_increment

    def transition_to_no_distributor_id(
        self,
        cluster_id: int,
        no_id_err_msg: str,
    ) -> None:
        """Register that submission failed with the central service.

                Args:
                    no_id_err_msg: The error msg from the executor when failed to obtain distributor
                        id.
                """
        app_route = f"/task_instance/{self.task_instance_id}/log_no_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"no_id_err_msg": no_id_err_msg},
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

    def _transition_to_error(self, error_message: str, error_state):
        if self.distributor_id is None:
            raise ValueError("distributor_id cannot be None during log_error")
        distributor_id = self.distributor_id
        logger.debug(f"log_error for distributor_id {distributor_id}")
        if not self.error_state:
            raise ValueError("cannot log error if self.error_state isn't set")

        if error_state == TaskInstanceStatus.UNKNOWN_ERROR:
            app_route = f"/task_instance/{self.task_instance_id}/log_unknown_error"
        else:
            app_route = f"/task_instance/{self.task_instance_id}/log_known_error"

        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "error_state": error_state,
                "error_message": self.error_msg,
                "distributor_id": distributor_id,
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

    def transition_to_unknown_error(self, error_message: str) -> None:
        """Register that an unknown error was discovered during reconciliation."""
        self._transition_to_error(error_message, self.error_state)

    def transition_to_resource_error(self, error_message: str) -> None:
        """Register that a resource error was discovered during reconciliation."""
        self._transition_to_error(error_message, self.error_state)

    def transition_to_error(self, error_message: str) -> None:
        """Register that a known error occurred during reconciliation."""
        self._transition_to_error(error_message, self.error_state)
