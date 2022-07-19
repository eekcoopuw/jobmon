"""The workflow run is an instance of a workflow."""
from __future__ import annotations

import getpass
import logging
import time
from typing import Optional

from jobmon import __version__
from jobmon.constants import WorkflowStatus
from jobmon.client.client_config import ClientConfig
from jobmon.exceptions import InvalidResponse, WorkflowNotResumable
from jobmon.requester import http_request_ok, Requester


logger = logging.getLogger(__name__)


class WorkflowRunFactory:
    """
    A utility class responsible for instantiating workflow run objects.

    This class sends the appropriate resume signals so that the parent workflow
    object is in a state where the newly created workflowrun is ready to run, either on
    resume or not.
    """

    # TODO: workflow run factory is for now mostly a placeholder for various functions
    # useful for instantiating workflow runs either from a workflow or from the CLI.
    # Might want to consider unifying the resume API more, think about how to handle
    # task resource caching and creation as well in order to resume.

    def __init__(self,
                 workflow_id: int,
                 requester: Optional[Requester] = None):
        self.workflow_id = workflow_id
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester
        self.workflow_is_resumable = False

    def set_workflow_resume(
        self, reset_running_jobs: bool = True, resume_timeout: int = 300
    ) -> None:
        """Set statuses of the given workflow ID's workflow to a resumable state.

        Move active workflow runs to hot/cold resume states, depending on reset_running_jobs.
        """
        if self.workflow_is_resumable:
            return
        app_route = f"/workflow/{self.workflow_id}/set_resume"
        self.requester.send_request(
            app_route=app_route,
            message={
                "reset_running_jobs": reset_running_jobs
            },
            request_type="post",
        )
        # Wait for the workflow to become resumable
        self.wait_for_workflow_resume(resume_timeout)

    def wait_for_workflow_resume(self, resume_timeout: int = 300) -> None:
        # previous workflow exists but is resumable. we will wait till it terminates

        wait_start = time.time()
        while not self.workflow_is_resumable:
            logger.info(
                f"Waiting for resume. "
                f"Timeout in {round(resume_timeout - (time.time() - wait_start), 1)}"
            )
            app_route = f"/workflow/{self.workflow_id}/is_resumable"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get"
            )

            self.workflow_is_resumable = bool(response.get("workflow_is_resumable"))
            if (time.time() - wait_start) > resume_timeout:
                raise WorkflowNotResumable(
                    "workflow_run timed out waiting for previous "
                    "workflow_run to exit. Try again in a few minutes."
                )
            else:
                sleep_time = round(float(resume_timeout) / 10.0, 1)
                time.sleep(sleep_time)

    def reset_task_statuses(self, reset_if_running: bool = True) -> None:
        """Sets the tasks associated with a workflow to the appropriate states."""
        self.wait_for_workflow_resume()

        self.requester.send_request(
            app_route=f"/task/{self.workflow_id}/set_resume_state",
            message={'reset_if_running': reset_if_running},
            request_type='post'
        )

    def create_workflow_run(
        self,
    ) -> WorkflowRun:
        """Workflow should at least have signalled for a resume at this point."""

        # create workflow run
        client_wfr = WorkflowRun(workflow_id=self.workflow_id, requester=self.requester)
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
        workflow_run_heartbeat_interval: int = 30,
        heartbeat_report_by_buffer: float = 3.1,
    ) -> None:
        """Initialize client WorkflowRun."""
        # set attrs
        # Note: move task bind to workflow, pass in workflow id here instead of workflow object
        self.workflow_id = workflow_id
        self.user = getpass.getuser()

        if requester is None:
            cc = ClientConfig.from_defaults()
            requester = Requester(cc.url, max_retries=cc.tenacity_max_retries)
        self.requester = requester
        self.heartbeat_interval = workflow_run_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self._workflow_run_id = None
        self._status = None

    @property
    def workflow_run_id(self) -> int:
        if not self._workflow_run_id:
            raise WorkflowNotResumable(
                "This workflow run was not yet bound successfully, "
                "cannot access workflow run id attribute."
            )
        return self._workflow_run_id

    @property
    def status(self) -> Optional[str]:
        if not self._status:
            raise WorkflowNotResumable(
                "This workflow run was not bound successfully, "
                "cannot access status attribute.")
        return self._status

    def bind(
        self
    ) -> None:
        """Link this workflow run with the workflow and add all tasks."""

        if self._workflow_run_id:
            return  # WorkflowRun already bound

        next_report_increment = (
            self.heartbeat_interval * self.heartbeat_report_by_buffer
        )

        # bind to database
        app_route = "/workflow_run"
        _, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow_id,
                "user": self.user,
                "jobmon_version": __version__,
                "next_report_increment": next_report_increment
            },
            request_type="post",
        )
        workflow_run_id = resp.get('workflow_run_id')
        if not workflow_run_id:
            raise WorkflowNotResumable(resp.get('err_msg'))
        self._workflow_run_id = workflow_run_id
        self._status = resp.get('status')

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
        self._status = status

    def __repr__(self) -> str:
        """A representation string for a client WorkflowRun instance."""
        return (
            f"WorkflowRun(workflow_id={self.workflow_id}, "
            f"workflow_run_id={self.workflow_run_id}"
        )
