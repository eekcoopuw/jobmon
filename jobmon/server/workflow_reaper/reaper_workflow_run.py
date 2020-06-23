from __future__ import annotations
from datetime import datetime, timedelta
from http import HTTPStatus as StatusCodes

from jobmon import __version__
from jobmon.exceptions import InvalidResponse
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.requests.requester import Requester
from jobmon.serializers import SerializeWorkflowRun
from jobmon.server.server_logging import jobmonLogging as logging


logger = logging.getLogger(__file__)


class ReaperWorkflowRun(object):
    def __init__(self, workflow_run_id: int, workflow_id: int, heartbeat_date: datetime,
                 requester: Requester):
        """
        Implementing workflow reaper behavior of workflow run

        Args
            workflow_run_id: id of workflow run object from db
            workflow_id: id of associated workflow
            heartbeat_date: the last time a workflow_run logged that it was alive in date
                format
        """
        self.workflow_run_id = workflow_run_id
        self.workflow_id = workflow_id
        self.heartbeat_date = heartbeat_date
        self._requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, requester: Requester) -> ReaperWorkflowRun:
        kwargs = SerializeWorkflowRun.kwargs_from_wire(wire_tuple)
        return cls(workflow_run_id=kwargs["id"],
                   workflow_id=kwargs["workflow_id"],
                   heartbeat_date=datetime.strptime(kwargs["heartbeat_date"],
                                                    '%a, %d %b %Y %H:%M:%S %Z'),
                   requester=requester)

    def to_wire(self) -> tuple:
        return SerializeWorkflowRun.to_wire(self.workflow_run_id,
                                            self.workflow_id,
                                            self.heartbeat_date)

    def has_lost_workflow_run(self, loss_threshold: int) -> bool:
        """Return a bool if the workflow_run is lost"""
        time_since_last_heartbeat = (datetime.now() - self.heartbeat_date)
        return time_since_last_heartbeat > timedelta(minutes=loss_threshold)

    def transition_to_error(self) -> str:
        # Transition workflow run to E
        app_route = f'/workflow_run/{self.workflow_run_id}/update_status'
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
                  f"Workflow #{self.workflow_id} transitioned to FAILED state" \
                  f"Workflow Run #{self.workflow_run_id} transitioned to ERROR state"
        logger.info(message)
        return message

    def transition_to_suspended(self) -> str:
        app_route = f'/workflow/{self.workflow_id}/suspend'
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {response}')
        # Notify Slack about the workflow transition
        message = f"{__version__} Workflow Reaper Transition" \
                  f"Workflow #{self.workflow_id} transitioned to SUSPENDED state"
        logger.info(message)
        return message

    def transition_to_aborted(self) -> str:
        """Retrieve workflow_run status and status_date of the runs newest
        task."""
        # Get workflow_runs current state and the status_date of it's newest task
        app_route = f'/workflow_run/{self.workflow_run_id}/aborted'
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='put')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from PUT '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {result}')
        if result["was_aborted"]:
            # Send a message to Slack saying that the transition happened
            message = f"{__version__} Workflow Reaper Transition" \
                      f"Workflow #{self.workflow_id} transitioned to ABORTED state" \
                      f"Workflow Run #{self.workflow_run_id} transitioned to ABORTED state"
            logger.info(message)
        else:
            message = ""
        return message
