from datetime import datetime, timedelta
from time import sleep
from typing import List

from jobmon.serializers import SerializeWorkflowRun, SerializeLatestTaskDate
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.client import shared_requester
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon import __version__

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    def __init__(self, loss_threshold=5, poll_interval=10,
                 wf_notification_sink=None, requester=shared_requester):
        logger.debug(logging.myself())

        if poll_interval < loss_threshold:
            logger.debug(f"poll_interval ({poll_interval} min) must exceed the "
                         f"loss threshold ({loss_threshold} min)")
        self._requester = requester
        self._loss_threshold = timedelta(minutes=loss_threshold)
        self._poll_interval = poll_interval
        self._wf_notification_sink = wf_notification_sink

    def _monitor_forever(self):
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
            logger.debug(f"Error in _monitor_forever() in workflow reaper: {e}")

    def _check_by_given_status(self, status):
        """Return all workflows that are in a specific state"""
        logger.debug(logging.myself())
        app_route = f"/workflow_run/{status}/status"
        _, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        return result

    def _check_for_resume_state(self):
        """Return all workflow_runs in COLD_RESUME or HOT_RESUME state"""
        logger.debug(logging.myself())
        result = self._check_by_given_status("C")
        cold_workflow_runs = [
            (
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["id"],
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["workflow_id"]
            ) for wire_tuple in result["workflow_runs"]
        ]

        result = self._check_by_given_status("H")
        hot_workflow_runs = [
            (
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["id"],
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["workflow_id"]
            ) for wire_tuple in result["workflow_runs"]
        ]
        cold_hot_workflow_runs = cold_workflow_runs + hot_workflow_runs

        return cold_hot_workflow_runs

    def _suspended_state(self):
        """Check if a workflow_run is in H or C state, if it is transition
        the associated workflow to SUSPENDED state"""
        logger.debug(logging.myself())

        # Get workflow_runs in H and C state
        workflow_runs = self._check_for_resume_state()

        # Transition workflows to SUSPENDED
        for wfr in workflow_runs:
            self._requester.send_request(
                app_route='/workflow',
                message={'wf_id': wfr[1],
                         'status': WorkflowStatus.SUSPENDED,
                         'status_date': str(datetime.utcnow())},
                request_type='put'
            )

            # Notify Slack about the workflow transition
            message = f"{__version__} Worfklow Reaper Transition" \
                      f"Workflow #{wfr[1]} transitioned to SUSPENDED state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _register_error_state(self, lost_wfrs):
        """Transitions workflow to FAILED state and workflow run to ERROR"""
        for wfr in lost_wfrs:
            # Transition workflow run to E
            app_route = f'/workflow_run/{wfr[0]}/update_status'
            self._requester.send_request(
                app_route=app_route,
                message={'status': WorkflowRunStatus.ERROR},
                request_type='put'
            )
            # Transition workflow to F
            self._requester.send_request(
                app_route='/workflow',
                message={'wf_id': wfr[2],
                         'status': WorkflowStatus.FAILED,
                         'status_date': str(datetime.utcnow())},
                request_type='put'
            )
            # Send a message to slack about the transitions
            message = f"{__version__} Worfklow Reaper Transition" \
                      f"Workflow #{wfr[2]} transitioned to FAILED state" \
                      f"Workflow Run #{wfr[0]} transitioned to ERROR state"
            logger.info(message)
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=message)

    def _has_lost_workflow_run(self, workflow_run):
        """Return a bool if the workflow_run is lost"""
        logger.debug(logging.myself())
        datetime_string = workflow_run[1]

        # Example string that DB returns Mon, 30 Mar 2020 22:23:58 GMT
        datetime_object = datetime.strptime(datetime_string, '%a, %d %b %Y %H:%M:%S %Z')
        time_since_last_heartbeat = (datetime.utcnow() -
                                     datetime_object)
        return time_since_last_heartbeat > self._loss_threshold

    def _get_active_workflow_runs(self):
        """Get all workflow_runs that currently have R state"""
        logger.debug(logging.myself())
        result = self._check_by_given_status("R")
        workflow_runs = [
            (
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["id"],
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["heartbeat_date"],
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["workflow_id"]
            ) for wire_tuple in result["workflow_runs"]
        ]
        return workflow_runs

    def _get_lost_worfklow_runs(self):
        """Return all workflows that have not logged a heartbeat in awhile"""
        logger.debug(logging.myself())
        workflow_runs = self._get_active_workflow_runs()
        return [wr for wr in workflow_runs if self._has_lost_workflow_run(wr)]

    def _error_state(self):
        """Get lost workflows and register them as error"""
        lost_wfrs = self._get_lost_worfklow_runs()
        self._register_error_state(lost_wfrs)

    def _set_wfr_to_aborted(self, workflow_id, workflow_run_id):
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

            self._requester.send_request(
                app_route='/workflow',
                message={'wf_id': workflow_id,
                         'status': WorkflowStatus.ABORTED,
                         'status_date': str(datetime.utcnow())},
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
        result = self._check_by_given_status("G")
        registered_workflow_runs = [
            (
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["id"],
                SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["workflow_id"]
            ) for wire_tuple in result["workflow_runs"]
        ]
        # Call method to validate/register if workflow run should be in A state
        for wfr in registered_workflow_runs:
            self._set_wfr_to_aborted(wfr[0], wfr[1])

