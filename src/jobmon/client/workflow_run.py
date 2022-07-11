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


class WorkflowRunFactory:
    """
    A utility class responsible for instantiating workflow run objects.

    This class sends the appropriate resume signals so that the parent workflow
    object is in a state where the newly created workflowrun is ready to run, either on
    resume or not.
    """

    def __init__(self, requester: Optional[Requester] = None):
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    def set_workflow_resume(
        self, workflow_id: int, reset_running_jobs: bool = True,
    ) -> None:
        """Set statuses of the given workflow ID's workflow to a resumable state.

        Move active workflow runs to hot/cold resume states, depending on reset_running_jobs.
        """
        app_route = f"/workflow/{workflow_id}/set_resume"
        self.requester.send_request(
            app_route=app_route,
            message={
                "reset_running_jobs": reset_running_jobs
            },
            request_type="post",
        )

    def workflow_is_resumable(self, workflow_id: int, resume_timeout: int = 300) -> None:
        # previous workflow exists but is resumable. we will wait till it terminates
        wait_start = time.time()
        workflow_is_resumable = False
        while not workflow_is_resumable:
            logger.info(
                f"Waiting for resume. "
                f"Timeout in {round(resume_timeout - (time.time() - wait_start), 1)}"
            )
            app_route = f"/workflow/{workflow_id}/is_resumable"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get"
            )

            workflow_is_resumable = response.get("workflow_is_resumable")
            if (time.time() - wait_start) > resume_timeout:
                raise WorkflowNotResumable(
                    "workflow_run timed out waiting for previous "
                    "workflow_run to exit. Try again in a few minutes."
                )
            else:
                sleep_time = round(float(resume_timeout) / 10.0, 1)
                time.sleep(sleep_time)

    def create_workflow_run(
        self,
        workflow_id: int,
        resume: bool = False,
        reset_running_jobs: bool = True,
        resume_timeout: int = 300,
    ) -> WorkflowRun:
        # raise error if workflow exists and is done
        if resume:
            self.set_workflow_resume(workflow_id, reset_running_jobs)
            self.workflow_is_resumable(resume_timeout)

        # create workflow run
        client_wfr = WorkflowRun(workflow_id)
        client_wfr.bind()

        return client_wfr


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
