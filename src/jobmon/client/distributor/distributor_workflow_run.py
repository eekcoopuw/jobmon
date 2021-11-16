from __future__ import annotations

import logging
from typing import Dict, List

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
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
        self._launched_array_task_instance_ids: List[int] = []
        self._running_array_task_instance_ids: List[int] = []

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
        pass

    @property
    def running_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id) for tid in
                self._running_array_task_instance_ids]

    @property
    def registered_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered array task_instances.

        These ids are stored on the array object.
        """
        task_instances: List[DistributorTaskInstance] = []
        for array in self._arrays.values():
            array_task_instances = [self._task_instances[tiid] for tiid in
                                    array._registered_array_task_instance_ids]
            task_instances.extend(array_task_instances)
        return task_instances

    @property
    def launched_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    @property
    def running_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    def get_queued_tasks(self, queued_tasks_bulk_query_size: int) -> List[DistributorTask]:
        """Retrieve a list of task that are in queued state"""
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

        tasks = [
            DistributorTask.from_wire(wire_tuple=task, requester=self.requester)
            for task in response["task_dcts"]
        ]
        return tasks

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

    def launch_task_instance(
        self,
        task_instance: DistributorTaskInstance,
        cluster: ClusterDistributor
    ):
        """
        submits a task instance on a given distributor.

        adds the new task instance to self.submitted_or_running_task_instances
        """
        pass

    def launch_array_instance(
        self,
        array: DistributorArray,
        cluster: ClusterDistributor
    ):
        """
        submits an array task on a given distributor
        adds the new task instances to self.running_array_task_instances
        """
        # Clear the registered tasks and move into running
        self._running_array_task_instance_ids.extend(array.registered_array_task_instance_ids)
        array.registered_array_task_instance_ids = []

        # Fetch the command
        command = cluster.build_array_worker_node_command(array.array_id)
        array_distributor_id = cluster.submit_array_to_batch_distributor(command)

