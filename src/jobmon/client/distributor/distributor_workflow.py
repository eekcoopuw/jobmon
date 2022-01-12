from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING, Union

from jobmon.constants import TaskInstanceStatus
from jobmon.client.distributor.status_processor import StatusProcessor

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_array import DistributorArray
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.distributor.distributor_task import DistributorTask
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun

logger = logging.getLogger(__name__)


class DistributorWorkflow(StatusProcessor):

    def __init__(self, workflow_id: int):
        self.workflow_id = workflow_id

        self.task_instances: Dict[int, DistributorTaskInstance] = {}
        self.workflow_runs: Dict[int, DistributorWorkflowRun] = {}

    def add_workflow_run(self, workflow_run: DistributorWorkflowRun):
        if workflow_run.workflow_id != self.workflow_id:
            raise ValueError(
                f"workflow_id mismatch. DistributorWorkflowRun={workflow_run.workflow_id}. "
                f"Workflow={self.workflow_id}."
            )
        self.workflow_runs[workflow_run.workflow_run_id] = workflow_run
        workflow_run.workflow = self

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.workflow_id != self.workflow_id:
            raise ValueError(
                f"workflow_id mismatch. TaskInstance={task_instance.workflow_id}. "
                f"Workflow={self.workflow_id}."
            )
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.workflow = self

    def get_metadata(self):
        # app_route = f"/array/{array_id}"
        # return_code, response = self.requester.send_request(
        #     app_route=app_route, message={}, request_type="get", logger=logger
        # )
        # if http_request_ok(return_code) is False:
        #     raise InvalidResponse(
        #         f"Unexpected status code {return_code} from POST "
        #         f"request through route {app_route}. Expected "
        #         f"code 200. Response content: {response}"
        #     )
        self.max_concurrently_running = 100

    def get_status_processors(self, status: str):
        if status == TaskInstanceStatus.launched_task_instances:
            return StatusProcessor(self.prepare_task_instance_batches_for_launch)

    @property
    def capacity(self) -> int:
        capacity = (
            self.max_concurrently_running
            - len(self.workflow_run.launched_task_instances)
            - len(self.workflow_run.running_task_instances)
        )
        return capacity

    def instantiate_task_instances(self, distributor_service: DistributorService) -> None:
        """Instantiate all queued task instances for this workflow."""
        # TODO: rename _n_queued
        new_work = {
            TaskInstanceStatus.QUEUED: [self.instantiate_queued_task_instances],
        }

        # TODO: should we consider capacity before instantiating queued tasks?
        new_task_instances = self.workflow_run.instantiate_queued_task_instances(
            distributor_service._n_queued
        )
        self._processed_task_instances.update(new_task_instances)

        # while the new task instance equal batch size get new work
        while len(new_task_instances) == self._n_queued:
            new_task_instances = self.workflow_run.instantiate_queued_task_instances(
                self._n_queued
            )
            self._processed_task_instances.update(new_task_instances)

        # if we instantiated any, then we need to add prepare for launch to new work queue
        if self._processed_task_instances:
            new_work[TaskInstanceStatus.LAUNCHED] = [self.get_task_instance_batches_for_launch]

        # set new work queue
        self._new_work = new_work

    def prepare_task_instance_batches_for_launch(
        self,
        distributor_service: DistributorService
    ) -> None:
        # compute the task_instances that can be launched
        # capacity numbers
        workflow_run_capacity = self.capacity
        array_capacity_lookup: Dict[int, int] = {}

        # loop through all instantiated instances while we have capacity
        instantiated_task_instances = list(self.instantiated_task_instances)
        eligable_task_instances: Set[DistributorTaskInstance] = set()
        while workflow_run_capacity > 0 and instantiated_task_instances:
            task_instance = instantiated_task_instances.pop(0)
            array_id = task_instance.array_id

            # lookup array capacity. if first iteration, compute it on the array class
            array_capacity = array_capacity_lookup.get(
                array_id, distributor_service.get_array(array_id).capacity
            )

            # add to eligable_task_instances set if there is capacity
            if array_capacity > 0:
                eligable_task_instances.add(task_instance)
                workflow_run_capacity -= 1
                array_capacity -= 1

            # set new array capacity
            array_capacity_lookup[array_id] = array_capacity

        # loop through all eligable task instance and cluster into batches
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

    def monitor_launched_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: purge queueing errors. check distributor for liveliness?
        pass

    def monitor_running_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: Can EQW happen at this stage. check distributor for liveliness?
        pass

    def process_errored_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: EQW, unknown error, other?
        pass
