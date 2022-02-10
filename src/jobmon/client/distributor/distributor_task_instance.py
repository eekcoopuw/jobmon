"""Task Instance object from the distributor's perspective."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeTaskInstance

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_array import DistributorArray
    from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
    from jobmon.client.distributor.distributor_task import DistributorTask
    from jobmon.client.distributor.distributor_workflow import DistributorWorkflow


logger = logging.getLogger(__name__)


class DistributorTaskInstance:
    """Object used for communicating with JSM from the distributor node."""

    def __init__(
        self,
        task_instance_id: int,
        workflow_run_id: int,
        status: str,
        requester: Requester
    ) -> None:
        """Initialization of distributor task instance.

        Args:
            task_instance_id (int): a task_instance_id
            workflow_run_id (int): a workflow_run_id
            requester (Requester, optional): a requester to communicate with
                the JSM. default is shared requester
        """
        self.task_instance_id = task_instance_id
        self.workflow_run_id = workflow_run_id
        self.status = status

        self._requested_resources: Dict = {}

        self.error_state = ""
        self.error_msg = ""

        self.requester = requester

    @property
    def workflow(self) -> DistributorWorkflow:
        return self._workflow

    @workflow.setter
    def workflow(self, val: DistributorWorkflow):
        self._workflow = val

    @property
    def array(self) -> DistributorArray:
        return self._array

    @array.setter
    def array(self, val: DistributorArray):
        self._array = val

    @property
    def task(self) -> DistributorTask:
        return self._task

    @task.setter
    def task(self, val: DistributorTask):
        self._task = val

    @property
    def array_batch(self) -> DistributorArrayBatch:
        return self._array_batch

    @array_batch.setter
    def array_batch(self, val: DistributorArrayBatch):
        self._array_batch = val

    @property
    def requested_resources(self) -> Dict:
        if not self._requested_resources:
            # TODO: actually load them
            self._requested_resources = {}

    @requested_resources.setter
    def requested_resources(self, val: Dict):
        self._requested_resources = val

    def transition_to_instantiated(self):
        """Transition current ti to initiated"""
        app_route = f"/task_instance/{self.task_instance_id}/instantiate_task_instance"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="post", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        kwargs = SerializeTaskInstance.kwargs_from_wire(response["task_instance"])
        self.task_id = kwargs["task_id"],
        self.array_id = kwargs["array_id"],
        self.workflow_id = kwargs["workflow_id"]
        self.status = TaskInstanceStatus.INSTANTIATED

    def transition_to_launched(self, next_report_increment: float) -> None:
        """Register the submission of a new task instance to a cluster."""

        app_route = f"/task_instance/{self.task_instance_id}/log_distributor_id"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "distributor_id": str(self.distributor_id),
                "subtask_id": str(self.subtask_id),
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

    def transition_to_no_distributor_id(
        self,
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

    def _transition_to_error(self, error_message: str, error_state: str):
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

        self.error_state = error_state

    def transition_to_unknown_error(
            self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that an unknown error was discovered during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

    def transition_to_resource_error(
            self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that a resource error was discovered during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

    def transition_to_error(
            self, error_message: str, error_state: str
    ) -> Tuple[Set[DistributorTaskInstance], List]:
        """Register that a known error occurred during reconciliation."""
        self._transition_to_error(error_message, error_state)
        return {self}, []

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
