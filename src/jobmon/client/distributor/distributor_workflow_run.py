from __future__ import annotations

import logging
from typing import Dict, List, Optional

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
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
        self.requester = requester

        # mapping of task_instance_id to DistributorTaskInstance
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._arrays: Dict[int, DistributorArray] = {}

        # lists of task_instance_ids in different states. used for property views into
        # self._task_instances dict. This gets refreshed from the db during
        # self.get_task_instance_status_updates
        self._registered_task_instance_ids: List[int] = []
        self._launched_task_instance_ids: List[int] = []
        self._running_task_instance_ids: List[int] = []

        # mapping of array_id to DistributorArray. stores the queue of task_instances to be
        # instantiated using the array strategy.
        # TODO: Rename to launched_task_instance_ids?
        self._launched_array_task_instance_ids: List[int] = []
        self._running_array_task_instance_ids: List[int] = []

        # Triaging queue
        self._triaging_queue = []

    @property
    def arrays(self) -> List[DistributorArray]:
        """Return a list of arrays"""
        pass

    @property
    def registered_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered task_instances"""
        return [self._task_instances[tiid] for tiid in self._registered_task_instance_ids]

    @property
    def launched_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id, self.requester) for tid in
                self._launched_task_instance_ids]

    @property
    def running_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id, self.requester) for tid in
                self._running_task_instance_ids]

    @property
    def registered_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered array task_instances.

        These ids are stored on the array object.
        """
        task_instances: List[DistributorTaskInstance] = []
        for array in self._arrays.values():
            array_task_instances = [self._task_instances[tiid] for tiid in
                                    array.instantiated_array_task_instance_ids]
            task_instances.extend(array_task_instances)
        return task_instances

    @property
    def launched_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id, self.requester) for tid in
                self._launched_array_task_instance_ids]

    @property
    def running_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id, self.requester) for tid in
                self._running_array_task_instance_ids]

    def get_queued_tasks(self, queued_tasks_bulk_query_size: int) -> \
            List[DistributorTask]:
        """Retrieve a list of task that are in queued state."""

        # Retrieve all tasks (up till the queued_tasks_bulk_query_size) that are in queued
        # state that are associated with the workflow.
        app_route = f"/workflow/{self.workflow_id}/queued_tasks/{queued_tasks_bulk_query_size}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        # Queued tasks associated with WF, concurrency limit hasn't been applied yet
        tasks = [
            DistributorTask.from_wire(wire_tuple=task, requester=self.requester)
            for task in response["task_dcts"]
        ]

    def get_array(self, array_id: int) -> DistributorArray:
        """Get an array from the array cache or from the database on first access

        Args:
            array_id: the array_id to get
        """
        try:
            array = self._arrays[array_id]
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
            self._arrays[array_id] = array
        return array

    def refresh_task_instance_statuses_from_db(self):
        """
        get task instance FSM updates.
        refresh FSM lists with any new states."""
        pass

    def refresh_array_statuses_from_db(self):
        """compute num done in array"""
        pass

    def register_task_instance(self, task: DistributorTask):
        """
        create task instances (task transitions from Queued -> Instantiating)

        attach task instances with Arrays to the associated array object
        add task instances without Arrays to self.registered_task_instances
        """
        # create task instance and add to registry
        task_instance = task.register_task_instance(self.workflow_run_id)
        self._task_instances[task_instance.task_instance_id] = task_instance

        # if it is an array task queue on the array
        if task.array_id is not None:
            array = self.get_array(task.array_id)
            array.queue_task_instance_id_for_array_launch(task_instance.task_instance_id)

        # otherwise add to the registered list
        else:
            self._registered_task_instance_ids.append(task_instance.task_instance_id)

    def transition_task_instance(self, array_id: Optional[int], task_instance_ids: List[int],
                                 distributor_id: int, status: TaskInstanceStatus):
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
        self._launched_task_instance_ids.append(task_instance.task_instance_id)
        resp = self.transition_task_instance(array_id=None,
                                             task_instance_ids=[task_instance.task_instance_id],
                                             distributor_id=distributor_id,
                                             status=TaskInstanceStatus.LAUNCHED)

        # Pull unsuccessful transitions from the response, and add to a triaging queue
        erroneous_ti_transitions = resp['erroneous_transitions']
        self._triaging_queue.extend(erroneous_ti_transitions)

        # Return ti_distributor_id
        return distributor_id

    def launch_array_instance(
        self,
        array: DistributorArray,
        cluster: ClusterDistributor
    ):
        """
        submits an array task on a given distributor
        adds the new task instances to self.running_array_task_instances
        """

        # all task instances associated with an array and a batch number
        ids_to_launch = array.instantiated_array_task_instance_ids
        array.add_batch_number_to_task_instances()

        # Fetch the command
        command = cluster.build_worker_node_command(task_instance_id=None,
                                                    array_id=array.array_id,
                                                    batch_number=array.batch_number - 1)

        array_distributor_id = cluster.submit_array_to_batch_distributor(
            command=command,
            name=array.name,  # TODO: array class should have a name in the client model
            requested_resources=array.requested_resources)

        # Clear the registered tasks and move into launched
        self._launched_array_task_instance_ids.extend(ids_to_launch)
        array.clear_registered_task_registry()

        resp = self.transition_task_instance(array_id=array.array_id,
                                             task_instance_ids=ids_to_launch,
                                             distributor_id=array_distributor_id,
                                             status=TaskInstanceStatus.LAUNCHED)

        # Pull unsuccessful transitions from the response, and add to a triaging queue
        erroneous_ti_transitions = resp['erroneous_transitions']
        self._triaging_queue.extend(erroneous_ti_transitions)

        return array_distributor_id

    def prep_tis_for_launch(self, instantiated_task_instances: List[DistributorTaskInstance], wf_max_concurrently_running: int) -> List[DistributorTaskInstance]:
        # Get all single and array tasks that are currently in launched and running
        # Assume all tasks are associated with an array, thus do not need to check
        # _launched_task_instance_ids and _running_task_instance_ids
        total_launched_running = len(self.launched_array_task_instances) + \
                              len(self.running_array_task_instances)

        # calculate workflow capacity
        workflow_capacity = wf_max_concurrently_running - total_launched_running

        num_instantiated_tis = len(instantiated_task_instances)
        instantiated_ti_index = 0
        launched_task_instances = []
        while workflow_capacity > 0:
            if instantiated_ti_index < num_instantiated_tis:
                ti = instantiated_task_instances[instantiated_ti_index]
                array = self.get_array(ti.array_id)

                # Don't allow the array limit to be greater than the workflow limit
                if array.max_concurrently_running > wf_max_concurrently_running:
                    array.max_concurrently_running = wf_max_concurrently_running

                array_launched_running = len(array.launched_array_task_instance_ids) + len(array.running_array_task_instance_ids)
                array_capacity = array.max_concurrently_running - array_launched_running
                if array_capacity > 0:
                    array.queue_task_instance_id_for_array_launch(ti.task_instance_id)
                    workflow_capacity -= 1

                    # Add to array launched and workflow launched list, remove from instantiated list
                    self._launched_array_task_instance_ids.append(ti.task_instance_id)
                    array.launched_array_task_instance_ids.append(ti.task_instance_id)
                    launched_task_instances.append(ti)

                instantiated_ti_index += 1

            else:
                logger.info("Workflow capacity is greater than the number of instantiated "
                            "task instances ready to be launched.")
                break

        return [x for x in instantiated_task_instances if x not in launched_task_instances]
