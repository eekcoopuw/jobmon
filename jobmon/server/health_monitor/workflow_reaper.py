from http import HTTPStatus as StatusCodes
from time import sleep
from typing import List

from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.client import shared_requester
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon import __version__
from jobmon.server.health_monitor.reaper_workflow_run import ReaperWorkflowRunResponse, \
    ReaperWorkflowRun
from jobmon.client.requests.requester import Requester
from jobmon.server.health_monitor.reaper_config import WorkflowReaperConfig
from jobmon.exceptions import InvalidResponse

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    def __init__(self,
                 poll_interval_minutes: int = None,
                 loss_threshold: int = None,
                 wf_notification_sink = None,
                 requester: Requester = shared_requester):

        logger.debug(logging.myself())
        config = WorkflowReaperConfig.from_defaults()

        # Set poll interval and loss threshold to config ones if nothing passed in
        self._poll_interval_minutes = config.poll_interval_minutes if poll_interval_minutes is None else poll_interval_minutes
        self._loss_threshold = config.loss_threshold if loss_threshold is None else loss_threshold
        self._requester = requester
        self._wf_notification_sink = wf_notification_sink

        if self._poll_interval_minutes < self._loss_threshold:
            logger.debug(f"poll_interval ({self._poll_interval_minutes} min) must exceed the "
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

    def _check_by_given_status(self, status: List[str]) -> ReaperWorkflowRunResponse:
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
        workflow_runs = ReaperWorkflowRunResponse.from_wire(result)
        return workflow_runs

    def _suspended_state(self) -> None:
        """Check if a workflow_run is in H or C state, if it is transition
        the associated workflow to SUSPENDED state"""
        logger.debug(logging.myself())

        # Get workflow_runs in H and C state
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["C", "H"])

        # Transition workflows to SUSPENDED
        for wfr in result.workflow_runs:
            app_route = f'/workflow/{wfr.workflow_id}/suspend'
            return_code, response = self._requester.send_request(
                app_route=app_route,
                message={},
                request_type='post'
            )
            if return_code != StatusCodes.OK:
                raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}'
                )
            # Notify Slack about the workflow transition
            message = f"{__version__} Workflow Reaper Transition" \
                      f"Workflow #{wfr.workflow_id} transitioned to SUSPENDED state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _register_error_state(self, lost_wfrs: List[ReaperWorkflowRun]) -> None:
        """Transitions workflow to FAILED state and workflow run to ERROR"""
        for wfr in lost_wfrs:
            # Transition workflow run to E
            app_route = f'/workflow_run/{wfr.workflow_run_id}/update_status'
            return_code, response = self._requester.send_request(
                app_route=app_route,
                message={'status': WorkflowRunStatus.ERROR},
                request_type='put'
            )
            if return_code != StatusCodes.OK:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from PUT '
                    f'request through route {app_route}. Expected '
                    f'code 200. Response content: {response}'
                )
            # Send a message to slack about the transitions
            message = f"{__version__} Workflow Reaper Transition" \
                      f"Workflow #{wfr.workflow_id} transitioned to FAILED state" \
                      f"Workflow Run #{wfr.workflow_run_id} transitioned to ERROR state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _get_lost_workflow_runs(self) -> List[ReaperWorkflowRun]:
        """Return all workflows that have not logged a heartbeat in awhile"""
        logger.debug(logging.myself())
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["R"])
        return [wr for wr in result.workflow_runs if wr.has_lost_workflow_run(self._loss_threshold)]

    def _error_state(self) -> None:
        """Get lost workflows and register them as error"""
        lost_wfrs = self._get_lost_workflow_runs()
        self._register_error_state(lost_wfrs)

    def _set_wfr_to_aborted(self, workflow_id: int, workflow_run_id: int) -> None:
        """Retrieve workflow_run status and status_date of the runs newest
        task."""
        # Get workflow_runs current state and the status_date of it's newest task
        app_route = f'/workflow_run/{workflow_run_id}/aborted'
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from GET '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {result}')
        if result["status"]:
            # Send a message to Slack saying that the transition happened
            message = f"{__version__} Workflow Reaper Transition" \
                      f"Workflow #{workflow_id} transitioned to ABORTED state" \
                      f"Workflow Run #{workflow_run_id} transitioned to ABORTED state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _aborted_state(self) -> None:
        """Get all workflow runs in G state and validate if they should be in
        A state"""

        # Get all wfrs in G state
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["G"])

        # Call method to validate/register if workflow run should be in A state
        for wfr in result.workflow_runs:
            self._set_wfr_to_aborted(wfr.workflow_run_id, wfr.workflow_id)

