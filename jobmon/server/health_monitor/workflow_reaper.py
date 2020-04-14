from datetime import datetime, timedelta
from time import sleep
from typing import List

from jobmon.serializers import SerializeWorkflowRun, SerializeLatestTaskDate
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.client import shared_requester
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon import __version__
from jobmon.server.health_monitor.reaper_workflow_run import ReaperWorkflowRunResponse, \
    ReaperWorkflowRun
from jobmon.server.health_monitor.notifiers import SlackNotifier
from jobmon.client.requests.requester import Requester

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    def __init__(self, poll_interval: int=10, loss_threshold: int=5,
                 wf_notification_sink: SlackNotifier=None,
                 requester: Requester=shared_requester):

        logger.debug(logging.myself())

        if poll_interval < loss_threshold:
            logger.debug(f"poll_interval ({poll_interval} min) must exceed the "
                         f"loss threshold ({loss_threshold} min)")
        self._requester = requester
        self._poll_interval = poll_interval
        self._loss_threshold = timedelta(minutes=loss_threshold)
        self._wf_notification_sink = wf_notification_sink


    def monitor_forever(self):
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
                sleep(self._poll_interval * 60)
        except RuntimeError as e:
            logger.debug(f"Error in monitor_forever() in workflow reaper: {e}")

    def _check_by_given_status(self, status: str) -> ReaperWorkflowRunResponse:
        """Return all workflows that are in a specific state"""
        logger.debug(logging.myself())
        app_route = f"/workflow_run_status"
        _, result = self._requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='post')
        workflow_runs = ReaperWorkflowRunResponse.from_wire(result)
        return workflow_runs

    def _suspended_state(self):
        """Check if a workflow_run is in H or C state, if it is transition
        the associated workflow to SUSPENDED state"""
        logger.debug(logging.myself())

        # Get workflow_runs in H and C state
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["C", "H"])

        # Transition workflows to SUSPENDED
        for wfr in result.workflow_runs:
            self._requester.send_request(
                app_route=f'/workflow/{wfr.workflow_id}/suspend',
                message={},
                request_type='post'
            )

            # Notify Slack about the workflow transition
            message = f"{__version__} Worfklow Reaper Transition" \
                      f"Workflow #{wfr.workflow_id} transitioned to SUSPENDED state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _register_error_state(self, lost_wfrs: ReaperWorkflowRunResponse):
        """Transitions workflow to FAILED state and workflow run to ERROR"""
        for wfr in lost_wfrs:
            # Transition workflow run to E
            app_route = f'/workflow_run/{wfr.workflow_run_id}/update_status'
            self._requester.send_request(
                app_route=app_route,
                message={'status': WorkflowRunStatus.ERROR},
                request_type='put'
            )

            # Send a message to slack about the transitions
            message = f"{__version__} Worfklow Reaper Transition" \
                      f"Workflow #{wfr.workflow_id} transitioned to FAILED state" \
                      f"Workflow Run #{wfr.workflow_run_id} transitioned to ERROR state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _has_lost_workflow_run(self, workflow_run: ReaperWorkflowRun) -> bool:
        """Return a bool if the workflow_run is lost"""
        logger.debug(logging.myself())
        datetime_string = workflow_run.heartbeat_date

        # Example string that DB returns Mon, 30 Mar 2020 22:23:58 GMT
        datetime_object = datetime.strptime(datetime_string, '%a, %d %b %Y %H:%M:%S %Z')
        time_since_last_heartbeat = (datetime.utcnow() -
                                     datetime_object)
        return time_since_last_heartbeat > self._loss_threshold

    def _get_lost_worfklow_runs(self) -> List[ReaperWorkflowRun]:
        """Return all workflows that have not logged a heartbeat in awhile"""
        logger.debug(logging.myself())
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["R"])
        return [wr for wr in result.workflow_runs if self._has_lost_workflow_run(wr)]

    def _error_state(self):
        """Get lost workflows and register them as error"""
        lost_wfrs = self._get_lost_worfklow_runs()
        self._register_error_state(lost_wfrs)

    def _set_wfr_to_aborted(self, workflow_id: int, workflow_run_id: int):
        """Retrieve workflow_run status and status_date of the runs newest
        task."""
        # Get workflow_runs current state and the status_date of it's newest task
        app_route = f'/workflow_run/{workflow_run_id}/aborted'
        _, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        wire_args = result["statuses"]
        status = SerializeLatestTaskDate.kwargs_from_wire(wire_args)["status"]
        status_date = SerializeLatestTaskDate.kwargs_from_wire(wire_args)["status_date"]

        # Get difference between current time and workflow run status_date
        datetime_object = datetime.strptime(status_date, '%a, %d %b %Y %H:%M:%S %Z')
        time_since_status = datetime.utcnow() - datetime_object

        # If difference is more than 2 minutes change workflow and wfr to A
        if time_since_status > timedelta(minutes=2) and status == WorkflowStatus.REGISTERED:
            app_route = f'/workflow_run/{workflow_run_id}/update_status'
            self._requester.send_request(
                app_route=app_route,
                message={'status': WorkflowRunStatus.ABORTED},
                request_type='put'
            )

            # Send a message to Slack saying that the transition happened
            message = f"{__version__} Worfklow Reaper Transition" \
                      f"Workflow #{workflow_id} transitioned to ABORTED state" \
                      f"Workflow Run #{workflow_run_id} transitioned to ABORTED state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _aborted_state(self):
        """Get all workflow runs in G state and validate if they should be in
        A state"""

        # Get all wfrs in G state
        result: ReaperWorkflowRunResponse = self._check_by_given_status(["G"])

        # Call method to validate/register if workflow run should be in A state
        for wfr in result.workflow_runs:
            self._set_wfr_to_aborted(wfr.workflow_run_id, wfr.workflow_id)

