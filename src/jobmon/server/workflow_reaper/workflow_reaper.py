"""Service to monitor and reap dead workflows."""
import logging
from time import sleep
from typing import List

from jobmon import __version__
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.server.workflow_reaper.notifiers import SlackNotifier
from jobmon.server.workflow_reaper.reaper_workflow_run import ReaperWorkflowRun

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    """Monitoring and reaping dead workflows."""

    _version = __version__

    _reaper_message = {
        WorkflowRunStatus.ERROR: (
            "{__version__} Workflow Reaper transitioned Workflow #{workflow_id} to FAILED "
            "state. Workflow Run #{workflow_run_id} transitioned to ERROR state."
        ),
        WorkflowRunStatus.TERMINATED: (
            "{__version__} Workflow Reaper transitioned Workflow #{workflow_id} to HALTED "
            "state. Workflow Run #{workflow_run_id} transitioned to TERMINATED state."
        ),
        WorkflowRunStatus.ABORTED: (
            "{__version__} Workflow Reaper transitioned Workflow #{workflow_id} to ABORTED "
            "state. Workflow Run #{workflow_run_id} transitioned to ABORTED state."
        )
    }

    def __init__(self, poll_interval_minutes: int, requester: Requester,
                 wf_notification_sink: SlackNotifier = None) -> None:
        """Initializes WorkflowReaper class with specified poll interval and slack info.

        Args:
            poll_interval_minutes(int): how often the WorkflowReaper should check the
                database and reap workflows.
            requester (Requester): requester to communicate with Flask.
            wf_notification_sink (SlackNotifier): Notifier to send reaper messages to Slack.
        """
        logger.info(
            f"WorkflowReaper initializing with: poll_interval_minutes={poll_interval_minutes},"
            f"requester_url={requester.url}"
        )

        self._poll_interval_minutes = poll_interval_minutes
        self._requester = requester
        self._wf_notification_sink = wf_notification_sink

    def monitor_forever(self) -> None:
        """The main part of the Worklow Reaper.

        Check if workflow runs should be in ABORTED, SUSPENDED, or ERROR state. Wait and do
        it again.
        """
        logger.info("Monitoring forever...")

        if self._wf_notification_sink is not None:
            self._wf_notification_sink(msg=f"Workflow Reaper v{__version__} is alive")
        try:
            while True:
                self._halted_state()
                self._aborted_state()
                self._error_state()
                sleep(self._poll_interval_minutes * 60)
        except RuntimeError as e:
            logger.debug(f"Error in monitor_forever() in workflow reaper: {e}")

    def _get_lost_workflow_runs(self, status: List[str]) -> List[ReaperWorkflowRun]:
        """Return all workflows that are in a specific state."""
        logger.info(f"Checking the database for workflow runs of status: {status}")

        app_route = "/client/lost_workflow_run"
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={'status': status, 'version': self._version},
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

    def _halted_state(self) -> None:
        """Check if a workflow_run needs to be transitioned to terminated state."""
        # Get workflow_runs in H and C state
        workflow_runs = self._get_lost_workflow_runs(["C", "H"])

        # Transition workflows to HALTED
        target_status = WorkflowRunStatus.TERMINATED
        for wfr in workflow_runs:
            status = wfr.reap(target_status)
            if status == target_status and self._wf_notification_sink is not None:
                message = self._reaper_message[status].format(
                    __version__=self._version, workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id
                )
                self._wf_notification_sink(msg=message)

    def _error_state(self) -> None:
        """Get lost workflows and register them as error."""
        workflow_runs = self._get_lost_workflow_runs(["R"])

        # Transitions workflow to FAILED state and workflow run to ERROR
        target_status = WorkflowRunStatus.ERROR
        for wfr in workflow_runs:
            status = wfr.reap(target_status)
            if status == target_status and self._wf_notification_sink is not None:
                message = self._reaper_message[status].format(
                    __version__=self._version, workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id
                )
                self._wf_notification_sink(msg=message)

    def _aborted_state(self) -> None:
        """Find workflows that should be in aborted state.

        Get all workflow runs in G state and validate if they should be in A state. Get all
        lost wfr in L state and set it to A
        """
        # Get all lost wfr in L
        workflow_runs = self._get_lost_workflow_runs(["L"])

        # Transitions workflow to A state and workflow run to A
        target_status = WorkflowRunStatus.ABORTED
        for wfr in workflow_runs:
            status = wfr.reap(target_status)
            if status == target_status and self._wf_notification_sink is not None:
                message = self._reaper_message[status].format(
                    __version__=self._version, workflow_id=wfr.workflow_id,
                    workflow_run_id=wfr.workflow_run_id
                )
                self._wf_notification_sink(msg=message)
