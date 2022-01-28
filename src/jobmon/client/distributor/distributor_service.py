from __future__ import annotations

from datetime import datetime
import logging
import time
import sys
from types import TracebackType
from typing import Dict, List, Optional, Set, Type

import tblib.pickling_support

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
from jobmon.client.distributor.distributor_command import DistributorCommand
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.exceptions import InvalidResponse, ResumeSet, WorkflowRunStateError
from jobmon.requester import http_request_ok, Requester


logger = logging.getLogger(__name__)

tblib.pickling_support.install()


class ExceptionWrapper(object):
    """Handle exceptions."""

    def __init__(self, ee: Exception) -> None:
        """Initialization of execution wrapper."""
        self.ee = ee
        self.type: Optional[Type[BaseException]]
        self.value: Optional[BaseException]
        self.tb: Optional[TracebackType]
        self.type, self.value, self.tb = sys.exc_info()

    def re_raise(self) -> None:
        """Raise errors and add their traceback."""
        raise self.ee.with_traceback(self.tb)


class DistributorService:

    def __init__(
        self,
        cluster: ClusterDistributor,
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

        # indexing of task instance by associated id
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._tasks: Dict[int, DistributorTask] = {}
        self._arrays: Dict[int, DistributorArray] = {}
        self._workflows: Dict[int, DistributorWorkflow] = {}

        # work queue
        self.distributor_commands: List[DistributorCommand] = []

        # order through which we processes work
        self._status_processing_order = [
            TaskInstanceStatus.QUEUED,
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
            TaskInstanceStatus.TRIAGING,
        ]
        self._command_generator_map = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.LAUNCHED: self._check_launched_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
        }

        # syncronization timings
        dt_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._last_status_sync_time = {
            status: dt_string for status in self._status_processing_order
        }
        self._last_heartbeat_time = datetime.now()

        # cluster API
        self.cluster = cluster

        # web service API
        self.requester = requester

    @property
    def _next_report_increment(self) -> float:
        return self._heartbeat_report_by_buffer * self._task_instance_heartbeat_interval

    def set_workflow_run(self, workflow_run_id: int):
        workflow_run = DistributorWorkflowRun(workflow_run_id, self.requester)
        self.workflow_run = workflow_run

    def run(self):
        keep_running = True
        while keep_running:
            self.process_next_status()

    def process_next_status(self):
        """"""
        try:
            status = self._status_processing_order.pop(0)

            # syncronize statuses from the db and get new work
            self._check_for_work(status)

            while self.distributor_commands:
                # check if we need to pause for a heartbeat
                time_diff = time.time() - self.workflow_run.last_heartbeat
                if time_diff > self._workflow_run_heartbeat_interval:
                    self.heartbeat()

                # get the first callable and run it
                distributor_command = self.distributor_commands.pop(0)
                distributor_command()

        finally:
            # update task mappings
            self._update_status_map(status)

            self._status_processing_order.append(status)

    def instantiate_task_instance(self, task_instance: DistributorTaskInstance) -> None:
        # add associations
        task_instance.transition_to_instantiated()
        self._task_instances[task_instance.task_instance_id] = task_instance
        self._get_task(task_instance.task_id).add_task_instance(task_instance)
        self._get_array(task_instance.array_id).add_task_instance(task_instance)
        self._get_workflow(task_instance.workflow_id).add_task_instance(task_instance)

    def launch_array_batch(self, array_batch: DistributorArrayBatch) -> None:
        # record batch info in db
        array_batch.prepare_array_batch_for_launch()

        # build worker node command
        command = self.cluster.build_worker_node_command(
            task_instance_id=None,
            array_id=array_batch.array_id,
            batch_number=array_batch.batch_number
        )
        try:
            # submit array to distributor
            array_batch.distributor_id = self.cluster.submit_array_to_batch_distributor(
                command=command,
                name=array_batch.name,
                requested_resources=array_batch.requested_resources,
                array_length=len(array_batch.task_instances)
            )

        except NotImplementedError:
            # create DistributorCommands to submit the launch if array isn't implemented
            for task_instance in array_batch.task_instances:
                distributor_command = DistributorCommand(self.launch_task_instance)
                self.distributor_commands.append(distributor_command)

        except Exception as e:
            # if other error, transition to No ID status
            for task_instance in array_batch.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.transition_to_no_distributor_id, no_id_err_msg=str(e)
                )
                self.distributor_commands.append(distributor_command)

        else:
            # if successful log a transition to launched
            for task_instance in array_batch.task_instances:
                distributor_command = DistributorCommand(
                    task_instance.transition_to_launched, self._next_report_increment
                )
                self.distributor_commands.append(distributor_command)

    def launch_task_instance(self, task_instance: DistributorTaskInstance) -> None:
        """
        submits a task instance on a given distributor.
        adds the new task instance to self.submitted_or_running_task_instances
        """
        # Fetch the worker node command
        command = self.cluster.build_worker_node_command(
            task_instance_id=task_instance.task_instance_id
        )

        # Submit to batch distributor
        try:
            task_instance.distributor_id = self.cluster.submit_to_batch_distributor(
                command=command,
                name=task_instance.name,
                requested_resources=task_instance.requested_resources
            )

        except Exception as e:
            task_instance.transition_to_no_distributor_id(no_id_err_msg=str(e))

        else:
            # move from register queue to launch queue
            task_instance.transition_to_launched(self._next_report_increment)

    def triage_error(self, task_instance: DistributorTaskInstance) -> None:
        r_value, r_msg = self.cluster.get_remote_exit_info(task_instance.distributor_id)
        task_instance.transition_to_error(r_msg, r_value)

    def log_task_instance_report_by_date(self) -> None:
        task_instances = self._task_instance_status_map[TaskInstanceStatus.LAUNCHED].union(
            self._task_instance_status_map[TaskInstanceStatus.RUNNING]
        )
        # 1) build maps between task_instances and distributor_ids
        # 2) log heartbeats for instances.

    def _check_for_work(self, status: str):
        """Got to DB to check the list tis status."""
        message = {
            "task_instance_ids": [task_instance.task_instance_id for task_instance in
                                  self._task_instance_status_map[status]],
            "status": status,
            "last_sync": self._last_status_sync_time[status]
        }
        app_route = f"/workflow_run/{self.workflow_run.workflow_run_id}/sync_status"
        return_code, result = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type='post'
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"{app_route} Returned={return_code}. Message={message}"
            )

        # mutate the statuses and update the status map
        status_updates: Dict[int, str] = result["status_updates"]
        for task_instance_id, status in status_updates.items():
            try:
                task_instance = self._task_instances[task_instance_id]

            except KeyError:
                task_instance = DistributorTaskInstance(
                    task_instance_id, self.workflow_run.workflow_run_id, status, self.requester
                )

            else:
                # remove from old status set
                previous_status = task_instance.status
                self._task_instance_status_map[previous_status].remove(task_instance)

                # change to new status and move to new set
                task_instance.status = status

            finally:
                self._task_instance_status_map[task_instance.status].add(task_instance)

        # update the last sync time
        self._last_status_sync_time[status] = result["time"]

        # generate new distributor commands from this status
        try:
            command_generator = self._command_generator_map[status]
            command_generator()
        except KeyError:
            # no command generators based on this status. EG: RUNNING
            pass

    def _update_status_map(self, status: str):
        """Given a status, update the internal status map"""
        task_instances = self._task_instance_status_map.pop(status)
        self._task_instance_status_map[status] = set()
        for task_instance in task_instances:
            self._task_instance_status_map[task_instance.status].add(task_instance)

    def _check_queued_for_work(self) -> None:
        queued_task_instances = self._task_instance_status_map[TaskInstanceStatus.QUEUED]
        for task_instance in queued_task_instances:
            self.distributor_commands.append(
                DistributorCommand(self.instantiate_task_instance, task_instance)
            )

    def _check_instantiated_for_work(self) -> None:
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
                array_id, self._get_array_capacity(array_id)
            )
            workflow_capacity = workflow_capacity_lookup.get(
                workflow_id, self._get_workflow_capacity(workflow_id)
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
        for array in arrays:
            # TODO: perhaps this is top level command. We need to record array batch number
            # on the array itself for the resume case.
            array_batches = array.create_array_batches(eligable_task_instances)

            for array_batch in array_batches:
                distributor_command = DistributorCommand(self.launch_array_batch, array_batch)
                self.distributor_commands.append(distributor_command)

    def _check_launched_for_work(self) -> None:
        array_batches: Set[DistributorArrayBatch] = set()
        launched_task_instances = self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        for task_instance in launched_task_instances:
            array_batches.add(task_instance.array_batch)

        for array_batch in array_batches:
            distributor_command = DistributorCommand(
                array_batch.get_queueing_errors,
                self.cluster
            )
            self.distributor_commands.append(distributor_command)

    def _check_triaging_for_work(self) -> None:
        """For TaskInstances with TRIAGING status, check the nature of no heartbeat,
        and change the statuses accordingly"""
        triaging_task_instances = self._task_instance_status_map[TaskInstanceStatus.TRIAGING]
        for task_instance in triaging_task_instances:
            distributor_command = DistributorCommand(self.triage_error, task_instance)
            self.distributor_commands.append(distributor_command)

    def _get_array(self, array_id: int) -> DistributorArray:
        """Get a task from the task cache or create it and add it to the initializing queue

        Args:
            task_id: the task to get
        """
        try:
            array = self._arrays[array_id]
        except KeyError:
            array = DistributorArray(array_id, requester=self.requester)
            self.distributor_commands.append(DistributorCommand(array.get_metadata))
            self._arrays[array.array_id] = array
        return array

    def _get_task(self, task_id: int) -> DistributorTask:
        """Get an array from the array cache or create it and add it to the initializing queue

        Args:
            array_id: the array to get
        """
        try:
            task = self._tasks[task_id]
        except KeyError:
            task = DistributorTask(task_id, requester=self.requester)
            self.distributor_commands.append(DistributorCommand(task.get_metadata))
            self._tasks[task.task_id] = task
        return task

    def _get_workflow(self, workflow_id) -> DistributorWorkflow:
        """Get a workflow from the workflow cache or create it and add it to the initializing

        Args:
            workflow_id: the workflow to get
        """
        try:
            workflow_id = self._workflows[workflow_id]
        except KeyError:
            workflow = DistributorWorkflow(workflow_id, requester=self.requester)
            self.distributor_commands.append(DistributorCommand(workflow.get_metadata))
            self._workflows[workflow.workflow_id] = workflow
        return workflow

    def _get_workflow_capacity(self, workflow_id: int) -> int:
        workflow = self._workflows[workflow_id]
        launched = workflow.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        )
        running = workflow.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.RUNNING]
        )
        concurrency = workflow.max_concurrently_running
        return concurrency - len(launched) - len(running)

    def _get_array_capacity(self, array_id: int) -> int:
        array = self._arrays[array_id]
        launched = array.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]
        )
        running = array.task_instances.intersection(
            self._task_instance_status_map[TaskInstanceStatus.RUNNING]
        )
        concurrency = array.max_concurrently_running
        return concurrency - len(launched) - len(running)
