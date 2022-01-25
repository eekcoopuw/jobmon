from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_command import DistributorCommand
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import DistributorUnexpected, InvalidResponse
from jobmon.requester import http_request_ok, Requester

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance


logger = logging.getLogger(__name__)


class DistributorService:

    def __init__(
        self,
        distributor: ClusterDistributor,
        requester: Requester,
        workflow_run_heartbeat_interval: int = 30,
        task_instance_heartbeat_interval: int = 90,
        heartbeat_report_by_buffer: float = 3.1,
        n_queued: int = 100,
        distributor_poll_interval: int = 10,
        worker_node_entry_point: Optional[str] = None
    ) -> None:

        # operational args
        self._worker_node_entry_point = worker_node_entry_point
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self._n_queued = n_queued
        self._distributor_poll_interval = distributor_poll_interval

        # indexing of task instanes by status
        self._task_instance_status_map: Dict[str, Set[DistributorTaskInstance]] = {}
        self._array_batches: Set[DistributorArrayBatch] = set()
        self._workflow_run: DistributorWorkflowRun

        # indexing of task instance by associated id
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._tasks: Dict[int, DistributorTask] = {}
        self._arrays: Dict[int, DistributorArray] = {}
        self._workflows: Dict[int, DistributorWorkflow] = {}

        # priority work queues
        self.status_processing_order = [
            TaskInstanceStatus.QUEUED,
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
        ]

        # distributor API
        self.distributor = distributor

        # web service API
        self.requester = requester

    @property
    def _next_report_increment(self) -> float:
        return self._heartbeat_report_by_buffer * self._task_instance_heartbeat_interval

    def run(self):
        keep_running = True
        while keep_running:
            self.process_next_status()

    def process_next_status(self):
        """"""
        try:
            status = self.status_processing_order.pop(0)

            # syncronize statuses from db
            self._refresh_status_from_db(status)

            distributor_commands = self._check_for_work(status)
            while distributor_commands:

                # check if we need to pause for a heartbeat
                self._check_heartbeat()

                # get the first callable and run it
                distributor_command = distributor_commands.pop(0)
                new_distributor_commands = distributor_command()

                # append new callables to the work queue
                distributor_commands.extend(new_distributor_commands)

        finally:
            # update task mappings
            self._update_status_map(status)

            self.status_processing_order.append(status)

    def set_workflow_run(self, workflow_run_id: int, workflow_id: int):
        workflow_run = DistributorWorkflowRun(workflow_run_id, workflow_id, self.requester)
        self.workflow_run = workflow_run

    def _refresh_status_from_db(self, status: str):
        """Got to DB to check the list tis status."""
        tids = [task_instance.task_instance_id for task_instance in
                self._task_instance_status_map[status]]
        message = {"task_instance_ids": tids, "status": status}
        return_code, res = self.requester.send_request(
            app_route="/task_instance/status_check",
            message=message,
            request_type='post'
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"/task_instance/status_check Returned={return_code}. Message={message}"
            )

        # mutate the statuses in memory
        unmatches: Dict[int, str] = res["unmatches"]
        for task_instance_id, status in unmatches.items():
            # remove from old status set
            task_instance = self._task_instances[task_instance_id]
            previous_status = task_instance.status
            self._task_instance_status_map[previous_status].remove(task_instance)

            # change to new status and move to new set
            task_instance.status = status
            self._task_instance_status_map[status].add(task_instance)

    def _check_for_work(self, status: str):
        work_generator_map = {
            TaskInstanceStatus.QUEUED: self._poll_for_queued_task_instances,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.LAUNCHED: self._check_for_queueing_errors,
        }
        work_generator = work_generator_map[status]
        return work_generator()

    def _check_heartbeat(self):
        pass

    def _update_status_map(self, status: str):
        """Given a status, update the internal status map"""
        task_instances = self._task_instance_status_map.pop(status)
        self._task_instance_status_map[status] = set()
        for task_instance in task_instances:
            self._task_instance_status_map[task_instance.status].add(task_instance)

    def _poll_for_queued_task_instances(self) -> None:
        """Instantiate all queued task instances for this workflow."""
        # TODO: rename _n_queued
        # TODO: should we consider capacity before instantiating queued tasks?

        processed_task_instances = set()
        new_task_instances = self.workflow_run.instantiate_queued_task_instances(
            self._n_queued
        )
        processed_task_instances.update(new_task_instances)

        # while the new task instance equal batch size get new work
        while len(new_task_instances) == self._n_queued:
            new_task_instances = self.workflow_run.instantiate_queued_task_instances(
                self._n_queued
            )
            processed_task_instances.update(new_task_instances)

        for task_instance in processed_task_instances:
            self.add_task_instance(task_instance)

    def _check_instantiated_for_work(self) -> List[DistributorCommand]:
        # compute the task_instances that can be launched

        instantiated_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.INSTANTIATED]
        )

        # capacity numbers
        workflow_capacity_lookup: Dict[int, int] = {}
        array_capacity_lookup: Dict[int, int] = {}

        # store arrays and eligable task_instances for later
        arrays: Set[DistributorArray] = set()
        eligable_task_instances: Set[DistributorTaskInstance] = set()

        # loop through all instantiated instances while we have capacity
        while instantiated_task_instances:
            task_instance = instantiated_task_instances.pop(0)
            array_id = task_instance.array_id
            workflow_id = task_instance.workflow_id

            # lookup array capacity. if first iteration, compute it on the array class
            array_capacity = array_capacity_lookup.get(
                array_id, self.get_array_capacity(array_id)
            )
            workflow_capacity = workflow_capacity_lookup.get(
                workflow_id, self.get_workflow_capacity(workflow_id)
            )

            # add to eligable_task_instances set if there is capacity
            if workflow_capacity > 0 and array_capacity > 0:
                eligable_task_instances.add(task_instance)

                # keep the set of arrays for later
                arrays.add(task_instance.array)

                # decrement the capacities
                workflow_capacity -= 1
                array_capacity -= 1

            # set the new capacities
            array_capacity_lookup[array_id] = array_capacity
            workflow_capacity_lookup[workflow_id] = workflow_capacity

        # loop through all arrays from earlier and cluster into batches
        distributor_commands: List[DistributorCommand] = []
        for array in arrays:
            # figure out batches for each array
            array_batches = array.create_array_batches(eligable_task_instances)

            for array_batch in array_batches:
                distributor_command = DistributorCommand(
                    array_batch.launch,
                    self.distributor,
                    self._next_report_increment
                )
                distributor_commands.append(distributor_command)

        return distributor_commands

    def _check_for_queueing_errors(self) -> List[DistributorCommand]:
        distributor_commands: List[DistributorCommand] = []
        launched_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        )
        array_batches: Set[DistributorArrayBatch] = set()
        for task_instance in launched_task_instances:
            array_batch = task_instance.array_batch
            array_batches.add(array_batch)

        for array_batch in array_batches:
            distributor_command = DistributorCommand(
                array_batch.get_queueing_errors,
                self.distributor
            )
            distributor_commands.append(distributor_command)

        return distributor_command

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        # add associations
        self._task_instances[task_instance.task_instance_id] = task_instance
        self._task_instance_status_map[task_instance.status].add(task_instance)

        self.get_task(task_instance.task_id).add_task_instance(task_instance)
        self.get_array(task_instance.array_id).add_task_instance(task_instance)
        self.get_workflow(task_instance.workflow_id).add_task_instance(task_instance)

    def get_array(self, array_id: int) -> DistributorArray:
        """Get a task from the task cache or create it and add it to the initializing queue

        Args:
            task_id: the task to get
        """
        try:
            array = self._arrays[array_id]
        except KeyError:
            array = DistributorArray(array_id, requester=self.requester)
            array.get_metadata()
            self._arrays[array.array_id] = array
        return array

    def get_task(self, task_id: int) -> DistributorTask:
        """Get an array from the array cache or create it and add it to the initializing queue

        Args:
            array_id: the array to get
        """
        try:
            task = self._tasks[task_id]
        except KeyError:
            task = DistributorTask(task_id, requester=self.requester)
            task.get_metadata()
            self._tasks[task.task_id] = task
        return task

    def get_workflow(self, workflow_id) -> DistributorWorkflow:
        """Get a workflow from the workflow cache or create it and add it to the initializing

        Args:
            workflow_id: the workflow to get
        """
        try:
            workflow_id = self._workflows[workflow_id]
        except KeyError:
            workflow = DistributorWorkflow(workflow_id, requester=self.requester)
            workflow.get_metadata()
            self._workflows[workflow.workflow_id] = workflow
        return workflow

    def get_workflow_capacity(self, workflow_id: int) -> int:
        workflow = self._workflows[workflow_id]
        launched = workflow.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        )
        running = workflow.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.RUNNING]
        )
        concurrency = workflow.max_concurrently_running
        return concurrency - len(launched) - len(running)

    def get_array_capacity(self, array_id: int) -> int:
        array = self._arrays[array_id]
        launched = array.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        )
        running = array.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.RUNNING]
        )
        concurrency = array.max_concurrently_running
        return concurrency - len(launched) - len(running)
