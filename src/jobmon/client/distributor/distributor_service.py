from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.requester import http_request_ok, Requester

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.status_processor import StatusProcessor

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
        self._workflow_run: DistributorWorkflowRun
        self._array_batch: Set[DistributorArrayBatch] = set()

        # indexing of task instance by id
        self._tasks: Dict[int, DistributorTask] = {}
        self._arrays: Dict[int, DistributorArray] = {}
        self._workflows: Dict[int, DistributorWorkflow] = {}

        # priority work queues
        self.status_processor_instance_map: Dict[str, List[StatusProcessor]] = {}
        self.status_processing_order = [
            TaskInstanceStatus.QUEUED,
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
        ]

        # distributor API
        self.distributor = distributor

        # web service API
        self.requester = requester

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

            status_processor_callables = self._check_for_work(status)
            while status_processor_callables:

                # check if we need to pause for a heartbeat
                self._check_heartbeat()

                # get the first callable and run it
                status_processor_callable = status_processor_callables.pop(0)
                processed_task_instances, new_callables = status_processor_callable()

                # append new callables to the work queue
                status_processor_callables.extend(new_callables)

                # update task mappings
                self._update_status_map(processed_task_instances)

        finally:
            self.status_processing_order.append(status)

    def set_workflow_run(self, workflow_run_id: int, workflow_id: int):
        workflow_run = DistributorWorkflowRun(workflow_run_id, workflow_id)
        workflow = self.get_workflow(workflow_id)
        workflow.add_workflow_run(workflow_run)
        self.workflow_run = workflow_run

    def _refresh_status_from_db(self, status: str):
        pass

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

    def _update_status_map(self, task_instances: Set[DistributorTaskInstance]):
        """Given a set of modified task instances, update the internal status map"""
        for task_instance_status, mapped_task_instances in self.status_map.items():
            self.status_map[task_instance_status] = mapped_task_instances - task_instances

        for task_instance in task_instances:
            self.status_map[task_instance.status].add(task_instance)

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

    def _check_instantiated_for_work(self) -> List[Callable]:
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
                array_id, task_instance.array.capacity
            )
            workflow_capacity = workflow_capacity_lookup.get(
                workflow_id, task_instance.workflow.capacity
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
        status_processors: List[Callable] = []
        for array in arrays:

            # limit eligable set to this array and store batches
            array_eligable = array.task_instances.intersection(eligable_task_instances)
            array_batch_sets: Dict[Tuple[int, int], Set[DistributorTaskInstance]] = {}
            for task_instance in array_eligable:
                key = (task_instance.array, task_instance.task_resources_id)
                if key not in array_batch_sets:
                    array_batch_sets[key] = set()
                array_batch_sets[key].add(task_instance)
                # Add to self._array_batch_sets?

            # construct the status processors for each batch
            for key, batch_set in array_batch_sets.items():
                array, task_resources_id = key
                array_batch = array.create_array_batch(task_resources_id, batch_set)
                status_processors.append(array_batch.launch_array_batch)

        return status_processors

    def _check_for_queueing_errors(self) -> List[Callable]:

        status_processors: List[Callable] = []
        launched_task_instances = list(
            self._task_instance_status_map[TaskInstanceStatus.LAUNCHED])
        array_batches: Set[DistributorArrayBatch] = set()
        for ti in launched_task_instances:
            array_batch = DistributorArrayBatch(
                array_id=ti.array_id,
                batch_number=ti.array_batch_num,
                task_resources_id=ti.task_resources_id
            )
            # TODO: array_batch = ti.array_batch
            # Array batch should be added as a property of task instance
            array_batches.add(array_batch)

        for array_batch in array_batches:
            status_processors.append(array_batch.get_queueing_errors)

        return status_processors

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        # add associations
        self.get_task(task_instance.task_id).add_task_instance(task_instance)
        self.get_array(task_instance.array_id).add_task_instance(task_instance)
        self.get_workflow(task_instance.workflow_id).add_task_instance(task_instance)
        self._task_instance_status_map[task_instance.status].add(task_instance)

    def get_array(self, array_id: int) -> DistributorArray:
        """Get a task from the task cache or create it and add it to the initializing queue

        Args:
            task_id: the task to get
        """
        try:
            array = self.arrays[array_id]
        except KeyError:
            array = DistributorArray(array_id, requester=self.requester)
            self.arrays[array.array_id] = array
            self.initializing_queue.append(array)
        return array

    def get_task(self, task_id: int) -> DistributorTask:
        """Get an array from the array cache or create it and add it to the initializing queue

        Args:
            array_id: the array to get
        """
        try:
            task = self.tasks[task_id]
        except KeyError:
            task = DistributorTask(task_id, requester=self.requester)
            self.tasks[task.task_id] = task
            self.initializing_queue.append(task)
        return task

    def get_workflow(self, workflow_id) -> DistributorWorkflow:
        """Get a workflow from the workflow cache or create it and add it to the initializing

        Args:
            workflow_id: the workflow to get
        """
        try:
            workflow_id = self.workflows[workflow_id]
        except KeyError:
            workflow = DistributorWorkflow(workflow_id, requester=self.requester)
            self.workflows[workflow.workflow_id] = workflow
            self.initializing_queue.append(workflow)
        return workflow

    def launch_task_instances(self, distributor_service: DistributorService):
        processed_task_instances: Set[DistributorTaskInstance] = set()
        task_instance_batches = self.workflow_run.get_task_instance_batches_for_launch()

        while task_instance_batches:
            task_instance_batch = task_instance_batches.pop(0)
            # get an element of the batch
            task_instance = next(iter(task_instance_batch))

            # TODO: this should be stored and accessed by cluster_id
            cluster = self.distributor

            # TODO: how do we translate task_resource_id into requested resources??? We can
            # pass the payload from the server when we get the task instance but that is
            # inefficient for arrays. Maybe just lookup in the submit method and keep a
            # registry on the workflow run?
            # task_resources = task_instance.task_resources_id
            if len(task_instance_batches) > 1:
                try:
                    task_instances = workflow_run.launch_task_instance_batch(
                        task_instance_batch, cluster
                    )
                    processed_task_instances.update(task_instances)
                except NotImplementedError:
                    # unpack set into single element tuples if not implemented by cluster
                    task_instance_batches.extend(list(zip(task_instance_batches)))
            else:
                # unpack single element
                task_instance = workflow_run.launch_task_instance(task_instance, cluster)
                processed_task_instances.add(task_instance)

        workflow_run.update_state_map(processed_task_instances)
