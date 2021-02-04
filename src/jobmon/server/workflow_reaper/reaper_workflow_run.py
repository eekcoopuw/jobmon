from __future__ import annotations
from datetime import datetime, timedelta
import logging

from jobmon import __version__
from jobmon.server.workflow_reaper.reaper_config import WorkflowReaperConfig
from jobmon.exceptions import InvalidResponse
from jobmon.constants import WorkflowRunStatus
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeWorkflowRun


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

    def has_lost_workflow_run(self, query_time: datetime, loss_threshold: int) -> bool:
        """Return a bool if the workflow_run is lost"""
        time_since_last_heartbeat = (query_time - self.heartbeat_date)
        return time_since_last_heartbeat > timedelta(minutes=loss_threshold)

    def transition_to_error(self) -> str:
        # Transition workflow run to E
        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/update_status'
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={'status': WorkflowRunStatus.ERROR},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from PUT '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}'
            )
        # Send a message to slack about the transitions
        message = f"{__version__} Workflow Reaper transitioned " \
                  f"Workflow #{self.workflow_id} to FAILED state. " \
                  f"Workflow Run #{self.workflow_run_id} transitioned to ERROR state"
        logger.info(message)
        return message

    def transition_to_suspended(self) -> str:
        cfg = WorkflowReaperConfig.from_defaults()
        time_out = cfg.workflow_run_heartbeat * cfg.task_instance_report_by_buffer

        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/suspend/{time_out}'
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {result}')
        if result["was_suspended"]:
            # Notify Slack about the workflow transition
            message = f"{__version__} Workflow Reaper transitioned " \
                      f"Workflow #{self.workflow_id} to SUSPENDED state"
            logger.info(message)
        else:
            message = ""

        return message

    def transition_to_aborted(self, aborted_seconds: int = (60 * 2)) -> str:
        """Retrieve workflow_run status and status_date of the runs newest

        Args:
            aborted_seconds: how long to wait for new bind activity (adding tasks) before
                declaring the workflow aborted.
        task."""
        # TODO: move aborted time into config file
        # Get workflow_runs current state and the status_date of it's newest task
        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/aborted/{aborted_seconds}'
        return_code, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(f'Unexpected status code {return_code} from PUT '
                                  f'request through route {app_route}. Expected '
                                  f'code 200. Response content: {result}')
        if result["was_aborted"]:
            # Send a message to Slack saying that the transition happened
            message = f"{__version__} Workflow Reaper transitioned " \
                      f"Workflow #{self.workflow_id} to ABORTED state. " \
                      f"Workflow Run #{self.workflow_run_id} transitioned to ABORTED state"
            logger.info(message)
        else:
            message = ""
        return message

    def __repr__(self):
        return (f"ReaperWorkflowRun(workflow_run_id={self.workflow_run_id}, "
                f"workflow_id={self.workflow_id}, heartbeat_date={self.heartbeat_date}")
