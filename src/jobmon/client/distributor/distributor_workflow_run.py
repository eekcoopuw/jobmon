from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.exceptions import DistributorUnexpected, InvalidResponse
from jobmon.requester import http_request_ok, Requester

logger = logging.getLogger(__name__)


class DistributorWorkflowRun:
    """
    This class is responsible for implementing workflow level bulk routes and tracking in
    memory state on the distributor.

    when polling from the database we should work in task space and translate into array
    space in memory where appropriate.

    when pushing to the database we should work in CommandType (Workflow/Array/Task) space
    """

    def __init__(self, workflow_run_id: int, requester: Requester):
        self.workflow_run_id = workflow_run_id
        self.requester = requester

    def _log_workflow_run_heartbeat(self, next_report_increment: float) -> None:
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "next_report_increment": next_report_increment,
                "status": WorkflowRunStatus.RUNNING,
            },
            request_type="post",
            logger=logger,
        )

    def _log_tis_heartbeat(self, tis: List) -> None:
        """Log heartbeat of given list of tis."""

        app_route = "/task_instance/log_report_by/batch"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"task_instance_ids": tis},
            request_type="post",
            logger=logger,
        )

    def __hash__(self):
        return self.workflow_run_id

    def __eq__(self, other: object) -> bool:
        """Check if the hashes of two WorkflowRuns are equivalent."""
        if not isinstance(other, DistributorWorkflowRun):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: DistributorWorkflowRun) -> bool:
        """Check if one hash is less than the hash of another."""
        return hash(self) < hash(other)
