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

    def __init__(self, workflow_run_id: int, workflow_id: int, requester: Requester):
        self.workflow_run_id = workflow_run_id
        self.workflow_id = workflow_id
        self.requester = requester

    @property
    def workflow(self) -> DistributorWorkflow:
        return self._workflow

    @workflow.setter
    def workflow(self, val: DistributorWorkflow):
        self._workflow = val

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

    def syncronize_status(self) -> None:
        """Log heartbeats."""
        # log heartbeats for tasks queued for batch execution and for the
        # workflow run
        logger.debug("Distributor: logging heartbeat")
        self._log_workflow_run_heartbeat()  # update wfr hearbeat time in DB
        self._log_tis_heartbeat([ti.task_instance_id for ti in self.task_instances])  # log heartbeat for all tis

        # check launching queue
        # sync with DB
        ti_dict = self.refresh_status_from_db(self._launched_task_instance_ids.ids, "B")
        if ti_dict:
            for tiid in ti_dict.keys():
                self._launched_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "R":
                    # move to running Q
                    self._running_task_instance_ids.add(tiid)
                elif ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] == "I":
                    raise Exception("No way this should happen.")
                else:
                    self.wfr_has_failed_tis = True
                    self._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
                    self._error_task_instance_ids.append(tiid)

        # sync with distributor
        # only check those unchanged in DB
        ti_dict = self.refresh_status_with_distributor(self._launched_task_instance_ids.ids, "B")
        second_log_heartbeat_list = []
        if ti_dict:
            for tiid in ti_dict.keys():
                if ti_dict[tiid] == "R":
                    # do nothing
                    pass
                else:
                    self._launched_task_instance_ids.pop(tiid)
                    second_log_heartbeat_list.append(tiid)
                    if ti_dict[tiid] == "D":
                        pass
                    elif ti_dict[tiid] == "I":
                        raise Exception("No way this should happen.")
                    else:
                        self.wfr_has_failed_tis = True
                        self._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
                        self._error_task_instance_ids.add(tiid)
            self.transition_task_instance(ti_dict)
        self._log_tis_heartbeat(second_log_heartbeat_list)

        # check running queue
        # sync with DB
        ti_dict = self.refresh_status_from_db(self._running_task_instance_ids.ids, "R")
        if ti_dict:
            for tiid in ti_dict.keys():
                self._running_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] in ("I", "B"):
                    raise Exception("No way this should happen.")
                else:
                    self._wfr_has_failed_tis = True
                    self._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
                    self._error_task_instance_ids.add(tiid)
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = self.refresh_status_with_distributor(self._running_task_instance_ids.ids, "R")
        if ti_dict:
            for tiid in ti_dict.keys():
                self._running_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] in ("B"):
                    raise Exception("The cluster much be crazy.")
                else:
                    self.wfr_has_failed_tis = True
                    self._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
                    self._error_task_instance_ids.add(tiid)
            self.transition_task_instance(ti_dict)
            self._log_tis_heartbeat(list(ti_dict.keys()))

    def instantiate_queued_task_instances(
        self,
        batch_size: int
    ) -> Set[DistributorTaskInstance]:
        """Retrieve a list of task that are in queued state."""
        # Retrieve all tasks (up till the queued_tasks_bulk_query_size) that are in queued
        # state that are associated with the workflow.
        app_route = f"/workflow/{self.workflow_run_id}/queued_tasks/{batch_size}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="post", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        # concurrency limit hasn't been applied yet
        task_instances: Set[DistributorTaskInstance] = set()
        for server_task_instance in response["task_instances"]:
            distributor_ti = DistributorTaskInstance.from_wire(
                wire_tuple=server_task_instance, requester=self.requester
            )
            task_instances.add(distributor_ti)
        return task_instances

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.workflow_run_id != self.workflow_run_id:
            raise ValueError(
                f"workflow_run_id mismatch. TaskInstance={task_instance.workflow_run_id}. "
                f"Array={self.workflow_run_id}."
            )
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.workflow_run = self
        self.state_map[task_instance.status].add(task_instance)

    @property
    def capacity(self) -> int:
        capacity = (
            self.max_concurrently_running
            - len(self.launched_task_instances)
            - len(self.running_task_instances)
        )
        return capacity

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
