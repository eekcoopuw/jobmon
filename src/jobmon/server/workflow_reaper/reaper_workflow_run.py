"""Reaper Behavior for a given Workflow Run."""
from __future__ import annotations

import logging

from jobmon.exceptions import InvalidResponse
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeWorkflowRun


logger = logging.getLogger(__file__)


class ReaperWorkflowRun(object):
    """Reaper Behavior for a given Workflow Run."""

    def __init__(self, workflow_run_id: int, workflow_id: int, requester: Requester):
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
        self._requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, requester: Requester) -> ReaperWorkflowRun:
        """Create Reaper Workflow Run object."""
        kwargs = SerializeWorkflowRun.kwargs_from_wire(wire_tuple)
        return cls(workflow_run_id=kwargs["id"],
                   workflow_id=kwargs["workflow_id"],
                   requester=requester)

    def reap(self, status: str) -> str:
        """Transition workflow run to error."""
        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/reap'
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from PUT '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}'
            )
        return response["status"]

    def __repr__(self):
        """Return formatted reaper workflow run data."""
        return (f"ReaperWorkflowRun(workflow_run_id={self.workflow_run_id}, "
                f"workflow_id={self.workflow_id}")
