import logging
from datetime import datetime
from time import sleep
from typing import List

from jobmon import __version__
from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester, http_request_ok
from jobmon.server.workflow_reaper.reaper_workflow_run import ReaperWorkflowRun

logger = logging.getLogger(__file__)


class WorkflowReaper(object):

    def __init__(self, poll_interval_minutes: int, loss_threshold: int, requester: Requester,
                 wf_notification_sink=None):

        logger.info(
            f"WorkflowReaper initializing with: poll_interval_minutes={poll_interval_minutes},"
            f"loss_threshold={loss_threshold}, requester_url={requester.url}"
        )

        # Set poll interval and loss threshold to config ones if nothing passed in
        self._poll_interval_minutes = poll_interval_minutes
        self._loss_threshold = loss_threshold
        self._requester = requester
        self._wf_notification_sink = wf_notification_sink

        if self._poll_interval_minutes < self._loss_threshold:
            raise ValueError(
                f"poll_interval ({self._poll_interval_minutes} min) must exceed the "
                f"loss threshold ({self._loss_threshold} min)")

    def monitor_forever(self) -> None:
        """The main part of the Worklow Reaper. Check if workflow runs should
        be in ABORTED, SUSPENDED, or ERROR state. Wait and do it again."""
        logger.info("Monitoring forever...")

        if self._wf_notification_sink is not None:
            self._wf_notification_sink(msg=f"Workflow Reaper v{__version__} is alive")
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
        logger.info(f"Checking the database for workflow runs of status: {status}")

        app_route = "/client/workflow_run_status"
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='get',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {result}')
        workflow_runs = []
        for wfr in result["workflow_runs"]:
            workflow_runs.append(ReaperWorkflowRun.from_wire(wfr, self._requester))

        if workflow_runs:
            logger.info(f"Found workflow runs: {workflow_runs}")
        return workflow_runs

    def _suspended_state(self) -> None:
        """Check if a workflow_run is in H or C state, if it is transition
        the associated workflow to SUSPENDED state"""
        # Get workflow_runs in H and C state
        workflow_runs = self._check_by_given_status(["C", "H"])

        # Transition workflows to SUSPENDED
        for wfr in workflow_runs:
            message = wfr.transition_to_suspended()
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _get_lost_workflow_runs(self) -> List[ReaperWorkflowRun]:
        # get time from db
        app_route = "/time"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='get',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}'
            )
        query_time = datetime.strptime(response['time'], '%Y-%m-%d %H:%M:%S')

        # Return all workflows that have not logged a heartbeat in awhile
        workflow_runs = self._check_by_given_status(["R"])

        # compare time
        lost_wfrs = [wfr for wfr in workflow_runs
                     if wfr.has_lost_workflow_run(query_time=query_time,
                                                  loss_threshold=self._loss_threshold)]
        return lost_wfrs

    def _error_state(self) -> None:
        """Get lost workflows and register them as error"""

        lost_wfrs = self._get_lost_workflow_runs()

        # Transitions workflow to FAILED state and workflow run to ERROR
        for wfr in lost_wfrs:
            # Transition workflow run to E
            message = wfr.transition_to_error()
            # Send a message to slack about the transitions
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _aborted_state(self, workflow_run_id: int = None, aborted_seconds: int = (60 * 2)
                       ) -> None:
        """Get all workflow runs in G state and validate if they should be in
        A state"""

        # Get all wfrs in G state
        workflow_runs = self._check_by_given_status(["G"])

        # Call method to validate/register if workflow run should be in A state
        for wfr in workflow_runs:
            if workflow_run_id:
                if wfr.workflow_run_id == workflow_run_id:
                    message = wfr.transition_to_aborted(aborted_seconds)
                    # Send a message to slack about the transitions
                    if self._wf_notification_sink and message:
                        self._wf_notification_sink(msg=message)
                    break
            else:
                message = wfr.transition_to_aborted(aborted_seconds)
                # Send a message to slack about the transitions
                if self._wf_notification_sink and message:
                    self._wf_notification_sink(msg=message)
