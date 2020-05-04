from http import HTTPStatus as StatusCodes
from time import sleep
from typing import List

from jobmon import __version__
from jobmon.client import shared_requester
from jobmon.client.requests.requester import Requester
from jobmon.exceptions import InvalidResponse
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.server.workflow_reaper.reaper_workflow_run import ReaperWorkflowRun
from jobmon.server.workflow_reaper.reaper_config import WorkflowReaperConfig

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    def __init__(self,
                 poll_interval_minutes: int = None,
                 loss_threshold: int = None,
                 wf_notification_sink=None,
                 requester: Requester = shared_requester):

        logger.debug(logging.myself())
        config = WorkflowReaperConfig.from_defaults()

        # Set poll interval and loss threshold to config ones if nothing passed in
        self._poll_interval_minutes = (
            config.poll_interval_minutes if poll_interval_minutes is None
            else poll_interval_minutes)
        self._loss_threshold = (
            config.loss_threshold if loss_threshold is None
            else loss_threshold)
        self._requester = requester
        self._wf_notification_sink = wf_notification_sink

        if self._poll_interval_minutes < self._loss_threshold:
            raise ValueError(
                f"poll_interval ({self._poll_interval_minutes} min) must exceed the "
                f"loss threshold ({self._loss_threshold} min)")

    def monitor_forever(self) -> None:
        """The main part of the Worklow Reaper. Check if workflow runs should
        be in ABORTED, SUSPENDED, or ERROR state. Wait and do it again."""
        logger.debug(logging.myself())
        if self._wf_notification_sink is not None:
            self._wf_notification_sink(
                msg=f"Workflow Reaper v{__version__} is alive"
            )
        try:
            while True:
                self._suspended_state()
                self._aborted_state()
                self._error_state()
                sleep(self._poll_interval_minutes * 60)
        except RuntimeError as e:
            logger.debug(f"Error in monitor_forever() in workflow reaper: {e}")

    def _check_by_given_status(self, status: List[str]) -> List[ReaperWorkflowRun]:
        """Return all workflows that are in a specific state"""
        logger.debug(logging.myself())
        app_route = f"/workflow_run_status"
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='get')

        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {result}')
        workflow_runs = []
        for wfr in result["workflow_runs"]:
            workflow_runs.append(ReaperWorkflowRun.from_wire(wfr, self._requester))
        return workflow_runs

    def _suspended_state(self) -> None:
        """Check if a workflow_run is in H or C state, if it is transition
        the associated workflow to SUSPENDED state"""
        logger.debug(logging.myself())

        # Get workflow_runs in H and C state
        workflow_runs = self._check_by_given_status(["C", "H"])

        # Transition workflows to SUSPENDED
        for wfr in workflow_runs:
            message = wfr.transition_to_suspended()
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _get_lost_workflow_runs(self) -> List[ReaperWorkflowRun]:
        # Return all workflows that have not logged a heartbeat in awhile
        workflow_runs = self._check_by_given_status(["R"])
        lost_wfrs = [wfr for wfr in workflow_runs
                     if wfr.has_lost_workflow_run(loss_threshold=self._loss_threshold)]
        return lost_wfrs

    def _error_state(self) -> None:
        """Get lost workflows and register them as error"""
        logger.debug(logging.myself())

        lost_wfrs = self._get_lost_workflow_runs()

        # Transitions workflow to FAILED state and workflow run to ERROR
        for wfr in lost_wfrs:
            # Transition workflow run to E
            message = wfr.transition_to_error()
            # Send a message to slack about the transitions
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _aborted_state(self) -> None:
        """Get all workflow runs in G state and validate if they should be in
        A state"""
        logger.debug(logging.myself())

        # Get all wfrs in G state
        workflow_runs = self._check_by_given_status(["G"])

        # Call method to validate/register if workflow run should be in A state
        for wfr in workflow_runs:
            message = wfr.transition_to_aborted()
            # Send a message to slack about the transitions
            if self._wf_notification_sink and message:
                self._wf_notification_sink(msg=message)
