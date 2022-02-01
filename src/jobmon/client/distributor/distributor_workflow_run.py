from __future__ import annotations

import logging
import time

from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester

logger = logging.getLogger(__name__)


class DistributorWorkflowRun:
    """
    This class is responsible for implementing workflow level bulk routes and tracking in
    memory state on the distributor.

    when polling from the database we should work in task space and translate into array
    space in memory where appropriate.

    when pushing to the database we should work in CommandType (Workflow/Array/Task) space
    """

    def __init__(self, workflow_run_id: int, requester: Requester):
        self.workflow_run_id = workflow_run_id

        self.status = ""
        self.last_heartbeat: float = time.time()
        self.requester = requester

    # def log_workflow_run_heartbeat(self, next_report_increment: float) -> None:
    #     app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
    #     return_code, response = self.requester.send_request(
    #         app_route=app_route,
    #         message={
    #             "next_report_increment": next_report_increment,
    #             "status": WorkflowRunStatus.RUNNING,
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

    #     self.status = response["message"]

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self.status = status

    def transition_to_instantiated(self) -> None:
        pass

    def transition_to_launched(self) -> None:
        pass

    def __hash__(self):
        return self.workflow_run_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two WorkflowRuns are equivalent."""
        if not isinstance(other, DistributorWorkflowRun):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: DistributorWorkflowRun) -> bool:
        """Check if one hash is less than the hash of another."""
        return hash(self) < hash(other)
