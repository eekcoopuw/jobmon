from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING

from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester

if TYPE_CHECKING:
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
        self._workflow_run_status_map: Dict[str, Set[DistributorWorkflowRun]] = {}

        # indexing of task instance by id
        self._tasks: Dict[int, DistributorTask] = {}
        self._arrays: Dict[int, DistributorArray] = {}
        self._workflows: Dict[int, DistributorWorkflow] = {}

        # poller set. Those objects that can poll the db for new work
        self.pollers: Set[DistributorWorkflow] = set()
        # list of tasks/arrays/workflows that need to be initialized post polling loop
        self.initializing_queue: List[Callable] = []

        # priority work queues
        self.status_processor_instance_map: Dict[str, List[StatusProcessor]] = {}
        self.status_processing_order = [
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
            TaskInstanceStatus.RUNNING,
            TaskInstanceStatus.UNKNOWN_ERROR,
        ]

        # distributor API
        self.distributor = distributor

        # web service API
        self.requester = requester

    def update_status_map(self, task_instances: Set[DistributorTaskInstance]):
        """Given a set of modified task instances, update the internal status map"""
        for task_instance_status, mapped_task_instances in self.status_map.items():
            self.status_map[task_instance_status] = mapped_task_instances - task_instances

        for task_instance in task_instances:
            self.status_map[task_instance.status].add(task_instance)

    def process_next_status(self):
        """"""
        try:
            status = self.status_processing_order.pop(0)
            status_processor_instances = self.status_processor_instance_map[status]
            while status_processor_instances and self._continue_processing:
                status_processor_instance = status_processor_instances.pop(0)
                processor_method = status_processor_instance.status_method_map[status]
                processor_method(self)

        finally:
            self.status_processing_order.append(status)

    def poll(self):
        for poller in self.pollers:
            poller.instantiated_task_instances(self)

    def set_workflow_run(self, workflow_run_id: int, workflow_id: int):
        workflow_run = DistributorWorkflowRun(workflow_run_id, workflow_id)
        workflow = self.get_workflow(workflow_id)
        workflow.add_workflow_run(workflow_run)

        # add to status map and register as an initializer
        self._workflow_run_status_map[workflow_run.status].add(workflow_run)
        self.pollers.add(workflow)

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
