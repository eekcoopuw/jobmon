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

    # TODO: workflow run factory is for now mostly a placeholder for various functions
    # useful for instantiating workflow runs either from a workflow or from the CLI.
    # Might want to consider unifying the resume API more, think about how to handle
    # task resource caching and creation as well in order to resume.

    def __init__(self, workflow_id: int, requester: Optional[Requester] = None):
        self.workflow_id = workflow_id
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester
        self._workflow_is_resumable = False

    @property
    def workflow_is_resumable(self):
        return self._workflow_is_resumable

    @workflow_is_resumable.setter
    def workflow_is_resumable(self, val: bool):
        self._workflow_is_resumable = val

    def set_workflow_resume(
        self, reset_running_jobs: bool = True, resume_timeout: int = 300
    ) -> None:
        """Set statuses of the given workflow ID's workflow to a resumable state.

        Move active workflow runs to hot/cold resume states, depending on reset_running_jobs.
        """
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
        resume: bool = False,
    ) -> WorkflowRun:
        """Workflow should at least have signalled for a resume at this point."""

        self.wait_for_workflow_resume()
        # create workflow run
        client_wfr = WorkflowRun(workflow_id=self.workflow_id, requester=self.requester)
        client_wfr.bind(resume=resume)

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
        self,
        resume: bool = False
    ) -> None:
        """Link this workflow run with the workflow and add all tasks.

        Resume: bool, determines whether the workflowrun is immediately transitioned to bound
        or not.
        """

        # bind to database
        app_route = "/workflow_run"
        _, resp = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow_id,
                "user": self.user,
                "jobmon_version": __version__,
                "resume": resume
            },
            request_type="post",
        )
        if not resp.workflow_run_id:
            raise WorkflowNotResumable(resp.err_msg)

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

    def __repr__(self) -> str:
        """A representation string for a client WorkflowRun instance."""
        return (
            f"WorkflowRun(workflow_id={self.workflow_id}, "
            f"workflow_run_id={self.workflow_run_id}"
        )
