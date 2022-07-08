"""The workflow run is an instance of a workflow."""
from __future__ import annotations

import getpass
import logging
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from jobmon import __version__
from jobmon.client.client_config import ClientConfig
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import InvalidResponse, WorkflowNotResumable
from jobmon.requester import http_request_ok, Requester


# avoid circular imports on backrefs
if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow


logger = logging.getLogger(__name__)


class WorkflowRun(object):
    """WorkflowRun enables tracking for multiple runs of a single Workflow.

    A Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(
        self,
        workflow_id: int,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialize client WorkflowRun."""
        # set attrs
        # Note: move task bind to workflow, pass in workflow id here instead of workflow object
        self.workflow_id = workflow_id
        self.user = getpass.getuser()

        if requester is None:
            requester = Requester(ClientConfig.from_defaults().url)
        self.requester = requester

        self._workflow_run_id = None

    @property
    def workflow_run_id(self) -> int:
        if not self._workflow_run_id:
            raise ValueError("Workflow Run not yet bound")
        return self._workflow_run_id

    @workflow_run_id.setter
    def workflow_run_id(self, val: int) -> None:
        self._workflow_run_id = val

    def bind(
        self
    ) -> None:
        """Link this workflow run with the workflow and add all tasks."""

        # get an id for this workflow run
        # TODO: If we remove linking, "G" may not even be necessary - will be moved straight to "B"
        self.workflow_run_id = self._register_workflow_run()
        # workflow was created successfully
        self.status = WorkflowRunStatus.REGISTERED

        # we did not successfully link. returned workflow_run_id is not the same as this ID
        # TODO: If linking state is removed this check is no longer needed
        # current_wfr = self.link()
        # if self.workflow_run_id != current_wfr_id:
        #
        #     raise WorkflowNotResumable(
        #         "There is another active workflow run already for workflow_id "
        #         f"({self.workflow_id}). Found previous workflow_run_id/status: "
        #         f"{current_wfr_id}/{current_wfr_status}"
        #     )

        self._update_status(WorkflowRunStatus.BOUND)

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self.status = status

    def _register_workflow_run(self) -> int:
        # bind to database
        app_route = "/workflow_run"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow_id,
                "user": self.user,
                "jobmon_version": __version__
            },
            request_type="post",
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"Invalid Response to {app_route}: {rc}")
        return response["workflow_run_id"]

    def _link_to_workflow(self, next_report_increment: float) -> Tuple[int, int]:
        app_route = f"/workflow_run/{self.workflow_run_id}/link"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"next_report_increment": next_report_increment},
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST  request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        return response["current_wfr"]

    def __repr__(self) -> str:
        """A representation string for a client WorkflowRun instance."""
        return (
            f"WorkflowRun(workflow_id={self.workflow_id}, "
            f"workflow_run_id={self.workflow_run_id}"
        )
