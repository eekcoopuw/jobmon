"""Task Instance object from the distributor's perspective."""
from __future__ import annotations

import logging
import time
from typing import Any, Optional, TYPE_CHECKING

from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeTaskInstance

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
    from jobmon.client.distributor.distributor_array import DistributorArray


logger = logging.getLogger(__name__)


class DistributorTaskInstance:
    """Object used for communicating with JSM from the distributor node."""

    def __init__(
        self,
        task_instance_id: int,
        workflow_run_id: int,
        task_id: int,
        workflow_id: int,
        array_id: int,
        task_resources_id: int,
        cluster_id: int,
        requester: Requester,
        distributor_id: Optional[int] = None,
        subtask_id: Optional[str] = None,
        array_batch_num: Optional[int] = None,
        array_step_id: Optional[int] = None,
        name: Optional[str] = None,
    ) -> None:
        """Initialization of distributor task instance.

        Args:
            task_instance_id (int): a task_instance_id
            workflow_run_id (int): a workflow_run_id
            cluster_type_id (int): the type of Cluster
            distributor_id (int, optional): the distributor_id associated with this
                task_instance for non array task; the parent distributor_id for array task
            subtask_id (int, optional): the distributor_id, it should be the same as distributor_id
                for non-array task
            requester (Requester, optional): a requester to communicate with
                the JSM. default is shared requester
        """
        self.task_instance_id = task_instance_id
        self.task_id = task_id
        self.workflow_id = workflow_id
        self.array_id = array_id
        self.task_resources_id = task_resources_id
        self.cluster_id = cluster_id

        self.workflow_run_id = workflow_run_id

        self.distributor_id = distributor_id
        if subtask_id is None and array_id is None:
            self.subtask_id = str(distributor_id)
        else:
            self.subtask_id = subtask_id

        self.array_batch_num = array_batch_num
        self.array_step_id = array_step_id
        self.name = name

        self.report_by_date: float

        self.error_state = ""
        self.error_msg = ""

        self.requester = requester

    @classmethod
    def from_wire(
        cls: Any, wire_tuple: tuple, requester: Requester
    ) -> DistributorTaskInstance:
        """Create an instance from json that the JQS returns.

        Args:
            wire_tuple: tuple representing the wire format for this
                task. format = serializers.SerializeTask.to_wire()
            cluster_type_id: which cluster type this task instance is
                being run on
            requester: requester for communicating with central services

        Returns:
            DistributorTaskInstance
        """
        kwargs = SerializeTaskInstance.kwargs_from_wire(wire_tuple)
        ti = cls(
            task_instance_id=kwargs["task_instance_id"],
            workflow_run_id=kwargs["workflow_run_id"],
            cluster_type_id=kwargs["cluster_type_id"],
            distributor_id=kwargs["distributor_id"],
            array_id=kwargs["array_id"],
            array_batch_num=kwargs["array_batch_num"],
            array_step_id=kwargs["array_step_id"],
            subtask_id=kwargs["subtask_id"],
            requester=requester,
        )
        return ti

    @property
    def workflow_run(self) -> DistributorWorkflowRun:
        return self._distributor_workflow_run

    @workflow_run.setter
    def workflow_run(self, val: DistributorWorkflowRun):
        self._distributor_workflow_run = val

    @property
    def array(self) -> DistributorArray:
        return self._distributor_array

    @array.setter
    def array(self, val: DistributorArray):
        self._distributor_array = val

    def transition_to_launched(
        self,
        cluster_id: int,
        distributor_id: int,
        subtask_id: str,
        next_report_increment: float,
    ) -> None:
        """Register the submission of a new task instance to a cluster."""
        self.distributor_id = distributor_id
        self.subtask_id = subtask_id

        app_route = f"/task_instance/{self.task_instance_id}/log_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "distributor_id": str(distributor_id),
                "subtask_id": str(subtask_id),
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

    def __hash__(self):
        return self.task_instance_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two tasks are equivalent."""
        if not isinstance(other, DistributorTaskInstance):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: DistributorTaskInstance) -> bool:
        """Check if one hash is less than the has of another Task."""
        return hash(self) < hash(other)
