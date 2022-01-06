from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

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

    def __init__(self, workflow_id: int, workflow_run_id: int, requester: Requester):
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.max_concurrently_running = 100
        self.requester = requester

        # registries
        self.task_instances: Dict[int, DistributorTaskInstance] = {}
        self.arrays: Dict[int, DistributorArray] = {}

        # lists of task_instance_ids in different states. used for property views into
        # self._task_instances dict. This gets refreshed from the db during
        # self.get_task_instance_status_updates
        self.state_map = {
            TaskInstanceStatus.INSTANTIATED: set(),
            TaskInstanceStatus.LAUNCHED: set(),
            TaskInstanceStatus.RUNNING: set(),
            TaskInstanceStatus.UNKNOWN_ERROR: set()
        }

    def get_array(self, array_id: int) -> DistributorArray:
        """Get an array from the array cache or from the database on first access

        Args:
            array_id: the array_id to get
        """
        try:
            array = self.arrays[array_id]
        except KeyError:
            app_route = f"/array/{array_id}"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get", logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f"Unexpected status code {return_code} from POST "
                    f"request through route {app_route}. Expected "
                    f"code 200. Response content: {response}"
                )
            array = DistributorArray.from_wire(response["array"], requester=self.requester)
            array.workflow_run = self
            self.arrays[array.array_id] = array
        return array

    def transition_task_instance(self, array_id: Optional[int], task_instance_ids: List[int],
                                 distributor_id: int, status: TaskInstanceStatus) -> Any:
        app_route = f"/task_instance/transition/{status}"
        rc, resp = self.requester.send_request(
            app_route=app_route,
            message={
                'array_id': array_id,
                # TODO: Will bulk update be too slow? Should we chunk?
                'task_instance_ids': task_instance_ids,
                'distributor_id': distributor_id
            },
            request_type='post'
        )
        if not http_request_ok(rc):
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {resp}"
            )
        return resp

    def _move_to_the_right_queue(self, tid: int, status: str):
        """Move the failed to transit ti to the right q."""
        if status == TaskInstanceStatus.INSTANTIATED:
            # move to register q
            self._registered_task_instance_ids.add(tid)
        elif status in {TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR}:
            self._launched_task_instance_ids.add(tid)
        elif status == TaskInstanceStatus.RUNNING:
            self._running_task_instance_ids.add(tid)
        elif status != TaskInstanceStatus.DONE:
            self._error_task_instance_ids.add(tid)

    def launch_task_instance(
        self,
        task_instance: DistributorTaskInstance,
        cluster: ClusterDistributor
    ):
        """
        submits a task instance on a given distributor.
        adds the new task instance to self.submitted_or_running_task_instances
        """
        # Fetch the worker node command
        command = cluster.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )
        # Submit to batch distributor
        distributor_id = cluster.submit_to_batch_distributor(
            command=command,
            name=task_instance.name,
            requested_resources=task_instance.requested_resources
        )

        # move from register queue to launch queue
        self._launched_task_instance_ids.add(task_instance.task_instance_id)
        self._registered_task_instance_ids.pop(task_instance.task_instance_id)
        resp = self.transition_task_instance(array_id=None,
                                             task_instance_ids=[task_instance.task_instance_id],
                                             distributor_id=distributor_id,
                                             status=TaskInstanceStatus.LAUNCHED)

        # Pull unsuccessful transitions from the response, and add to a triaging queue
        erroneous_tis: Dict[int, str] = resp['erroneous_transitions']
        if len(erroneous_tis) > 0:
            self._move_to_the_right_queue(task_instance.task_instance_id,
                                          erroneous_tis[str(task_instance.task_instance_id)])

        # Return ti_distributor_id
        return distributor_id

    def launch_array_instance(
        self,
        array: DistributorArray,
        cluster: ClusterDistributor
    ):
        """
        submits an array task on a given distributor
        adds the new task instances to self.launched_task_instances
        """

        # all task instances associated with an array and a batch number
        ids_to_launch = array.instantiated_array_task_instance_ids
        batch_num = array.add_batch_number_to_task_instances()
        # update distributor task instance array_batch_num and array_step_id
        for idx, tid in enumerate(sorted(ids_to_launch)):
            self._map.get_DistributorTaskInstance_by_id(tid).array_batch_num = batch_num
            # Increment index by 1 since we use 1-indexing in the worker node.
            # This will be synced with the database, since we are using the same algorithm.
            self._map.get_DistributorTaskInstance_by_id(tid).array_step_id = idx + 1

        # Fetch the command
        #
        command = cluster.build_worker_node_command(task_instance_id=None,
                                                    array_id=array.array_id,
                                                    batch_number=array.batch_number - 1)

        array_distributor_id = cluster.submit_array_to_batch_distributor(
            command=command,
            name=array.name,  # TODO: array class should have a name in the client model GBDSCI-4184
            requested_resources=array.requested_resources,
            array_length=len(ids_to_launch))

        # Clear the registered tasks and move into launched
        self._launched_task_instance_ids.extend(ids_to_launch)
        # remove from workflowrun registered list
        self._registered_task_instance_ids.remove(ids_to_launch)
        # remove from array registered list
        array.clear_registered_task_registry()

        resp = self.transition_task_instance(array_id=array.array_id,
                                             task_instance_ids=ids_to_launch,
                                             distributor_id=array_distributor_id,
                                             status=TaskInstanceStatus.LAUNCHED)

        # Pull unsuccessful transitions from the response, and add to a triaging queue
        erroneous_tis = resp['erroneous_transitions']
        for id in erroneous_tis.keys():
            self._move_to_the_right_queue(id, erroneous_tis[id])

        return array_distributor_id

    def get_ready_to_launch_batches(self):
        # compute the task_instances that can be launched
        eligable_task_instances = self._ready_to_launch

        task_instance_batches: List[Set[DistributorTaskInstance]] = []
        while eligable_task_instances:
            # pick one task instance out of the eligable set. Find any other task instances
            # that have compatible parameters and can be launched simultaneously
            task_instance = next(iter(eligable_task_instances))
            task_resources_id = task_instance.task_resources_id
            array = self.get_array(task_instance.array_id)
            task_instance_batch = set([
                task_instance for task_instance
                in array.instantiated_task_instances.intersection(eligable_task_instances)
                if task_instance.task_resources_id == task_resources_id
            ])

            # remove all members of this batch from eligable set and append to return list
            eligable_task_instances = eligable_task_instances - task_instance_batch
            task_instance_batches.append(task_instance_batch)

        return task_instance_batches

    @property
    def task_instance_heartbeat_interval(self) -> int:
        return self._task_instance_heartbeat_interval

    def _log_workflow_run_heartbeat(self) -> None:
        next_report_increment = (
            self.task_instance_heartbeat_interval * self.report_by_buffer
        )
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

    def refresh_status_from_db(self, tids: list, status: str) -> Dict[int: str]:
        """Got to DB to check the list tis status."""
        rc, res = self.requester._send_request(
            app_route="/task_instance/status_check",
            message={"task_instance_ids": tids,
                     "status": status},
            request_type='post'
        )
        if rc != 200:
            raise DistributorUnexpected(f"/task_instance/status_check returns "
                                        f"{rc} for [{tids}] status {status}")
        unmatches = res["unmatches"]
        return {int(id): unmatches[id] for id in unmatches.keys()}

    def refresh_status_with_distributor(self, tids: list, status: str) -> Dict[int, str]:
        """Go to the distributor to check the list tis status.

           Return: a dict of {task_instanc_id: status}

        TODO: Return a list of tis with status doesn't match status.
              The cluster plugin should return a dict of {subtaskid: status},
              use the map object to turn it into {tiid: status}.

              GBDSCI-4179
        """
        pass

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

    def create_instances(self, batch_size: int) -> List[DistributorTaskInstance]:
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
        task_instances = []
        for server_task_instance in response["task_instances"]:
            distributor_ti = DistributorTaskInstance.from_wire(
                wire_tuple=server_task_instance, requester=self.requester
            )
            task_instances.append(distributor_ti)
            self.add_task_instance(distributor_ti)
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

    @property
    def _ready_to_launch(self) -> Set[DistributorTaskInstance]:
        # capacity numbers
        workflow_run_capacity = self.capacity
        array_capacity_lookup: Dict[int, int] = {}

        # loop through all instantiated instances while we have capacity
        instantiated_task_instances = list(self.instantiated_task_instances)
        ready_to_launch: Set[DistributorTaskInstance] = set()
        while workflow_run_capacity > 0 and instantiated_task_instances:
            task_instance = instantiated_task_instances.pop(0)
            array_id = task_instance.array_id

            # lookup array capacity. if first iteration, compute it on the array class
            array_capacity = array_capacity_lookup.get(
                array_id, self.get_array(array_id).capacity
            )

            # add to ready_to_launch set if there is capacity
            if array_capacity > 0:
                ready_to_launch.add(task_instance)
                workflow_run_capacity -= 1
                array_capacity -= 1

            # set new array capacity
            array_capacity_lookup[array_id] = array_capacity

        return ready_to_launch
