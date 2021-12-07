from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester

logger = logging.getLogger(__name__)


class _arraysMap:
    """A dict object to hold all arrays in the wfr."""
    def __init__(self):
        self._all_data = dict()
        self._all_value = []

    def add_new_element(self, array_id: int, array_batch_id: int, tid: int):
        """Store two level indexes and value.

        For example, array_id=1, array_batch_id=2, tid=3:
            self.add_new_element_three_dimentaion(1, 2, 3)
        """
        x, y, z = array_id, array_batch_id, tid

        if x in self._all_data.keys():
            if type(self._all_data[x]) == dict:
                if y in self._all_data[x].keys():
                    self._all_data[x][y].append(z)
                else:
                    self._all_data[x][y] = [z]
        else:
            temp_dict = dict()
            temp_dict[y] = [z]
            self._all_data[x] = temp_dict
        self._all_value.append(z)

    def get_tis_by_array(self, array_id: int) -> List[int]:
        if array_id in self._all_data.keys():
            return_list = []
            for v in self._all_data[array_id].values():
                return_list.extend(v)
            return return_list
        else:
            return []

    def get_tis_by_array_batch(self, array_id: int, array_batch_id: int) -> List[int]:
        if array_id in self._all_data.keys():
            if array_batch_id in self._all_data[array_id].keys():
                return self._all_data[array_id][array_batch_id]
            else:
                return []
        else:
            return []

class WorkflowRunMaps:
    """
    This class holds the datastructure to map wfr ID, distributor ID, and DistributorTaskInstance.

    For example:
        *********************************************************************************************
        * tid   * distributor ID    * subtask_id   * array_id    * array_batch_id  * array_step_id  *
        * 1     * 1                 * 1            * null        * null            * null           *
        * 2     * 2                 * 2.1          * 1           * 1               * 1              *
        * 3     * 2                 * 2.2          * 1           * 1               * 2              *
        *********************************************************************************************
        _map_tiid_subtid: {1: "1", 2: "2.1", 3: "2.2"}
        _map_subtid_tiid: {"1": 1, "2.1": 2, "2.2": 1}
        _map_did_tiid: {1: [1], 2: [{1: [2, 3]}]}  # multiple way map
        _map_aid_tiid: {1: [2, 3]}

    """

    def __init__(self):
        #  map of task instance id and DistributorTaskInstance
        self._map_tiid_DistributorTaskInstance = dict()
        # map of task instance id and subtaskid
        #self._map_tiid_did = dict()
        self._map_tiid_subtid = dict()
        # map of subtaskid and task instance id
        self._map_subtid_tiid = dict()
        # map of distributor id and task instance id; one to many
        self._map_did_tiid = dict()
        # map for array and task instance list
        self._map_arrays = _arraysMap()
        # map all arrays and DistributorArray
        self._map_aid_DistributorArray = dict()
        # map distributorid to array array batch
        self._map_did_array_batch = dict()

    def add_DistributorArray(self, a: DistributorArray):
        self._map_aid_DistributorArray[a.array_id] = a

    def add_DistributorTaskInstance(self, ti: DistributorTaskInstance):
        tiid = ti.task_instance_id
        distributorid = ti.distributor_id
        subtaskid = ti.subtask_id
        # add ti to array in the map
        if ti.array_id is not None:
            self._map_arrays.add_new_element(ti.array_id, ti.array_batch_id, tiid)
        if tiid is None:
            #  TODO: task instance ID should not be None at this point. Handle it better.
            pass
        else:
            self._map_tiid_DistributorTaskInstance[tiid] = ti
            self._map_tiid_subtid[tiid] = subtaskid
            self._map_subtid_tiid[subtaskid] = tiid
            if distributorid is not None:
                if distributorid in self._map_did_tiid.keys():
                    self._map_did_tiid[distributorid].add(tiid)
                else:
                    self._map_did_tiid[distributorid] = {tiid}
                # distributor id to (array_id, array_batch_id) should be unique
                if distributorid not in self._map_did_array_batch.keys():
                    self._map_did_array_batch[distributorid] = (ti.array_id, ti.array_batch_id)

    def get_DistributorTaskInstance_by_id(self, tid: int) -> DistributorTaskInstance:
        """Return DistributorTaskInstance by task instance id."""
        return self._map_tiid_DistributorTaskInstance[tid]

    def get_DistributorTaskInstance_by_did(self, did: int) -> List[DistributorTaskInstance]:
        """Return DistributorTaskInstance by distributor id."""
        tiids = list(self._map_did_tiid[did])
        return [self._map_tiid_DistributorTaskInstance[tiid] for tiid in tiids]

    def get_DistributorTaskInstance_by_subtaskid(self, subtaskid: str) -> DistributorTaskInstance:
        """Return DistributorTaskInstance by sub task id (distributor id for sub task)."""
        tid = self._map_subtid_tiid[subtaskid]
        return self._map_tiid_DistributorTaskInstance[tid]

    def get_task_instance_ids(self) -> List[int]:
        """Returns all task instance ids."""
        return list(self._map_tiid_subtid.keys())

    def get_task_instances(self) -> List[DistributorTaskInstance]:
        """Return all task instances."""
        return self._map_tiid_DistributorTaskInstance.values()

    def get_DistributorArray(self, a_id: int) -> DistributorArray:
        """Return DistributorArray by array id."""
        return self._map_aid_DistributorArray[a_id]

    def get_array_ids(self) -> List[int]:
        """Return all array ids."""
        return list(self._map_aid_DistributorArray.keys())

    def get_arrays(self) -> List[DistributorArray]:
        """Return all arrays."""
        return list(self._map_aid_DistributorArray.values())

    def get_array_DistributorTaskInstance(self, array_id: int) -> List[DistributorTaskInstance]:
        tis = self._map_arrays.get_tis_by_array(array_id)
        return [self._map_tiid_DistributorTaskInstance[tiid] for tiid in tis]

    def get_array_batch_DistributorTaskInstance(self, array_id: int, batch_id: int) -> List[DistributorTaskInstance]:
        tis = self._map_arrays.get_tis_by_array_batch(array_id, batch_id)
        return [self._map_tiid_DistributorTaskInstance[tiid] for tiid in tis]

    def get_DistributorArray_by_tiid(self, tiid: int) -> DistributorArray:
        return self._map_aid_DistributorArray[self.get_DistributorTaskInstance_by_id(tiid).array_id]


class _tiList:
    """This is the linked list for ti in different states."""

    def __init__(self):
        self.tis = set()

    @property
    def ids(self) -> List[int]:
        return list(self.tis)

    @property
    def length(self) -> int:
        return len(self.tis)

    def add(self, i: int):
        self.tis.add(i)

    def extend(self, ids: List[int]):
        for i in ids:
            self.add(i)

    def pop(self, i: int) -> Optional[int]:
        """Remove one ti."""
        if i in self.tis:
            self.tis.remove(i)
            return i
        else:
            return None

    def remove(self, tis: List[int]) -> List:
        """Remove a list of tis."""
        return_list = []
        for ti in tis:
            t = self.pop(ti)
            if t:
                return_list.append(t)
        return return_list

    def get_distributortis(self, map: WorkflowRunMaps) -> List[DistributorTaskInstance]:
        return_list = set()
        for ti in self.tis:
            return_list.add(map.get_DistributorTaskInstance_by_id(ti))
        return list(return_list)

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

        # create the map of task_instance_id to DistributorTaskInstance and array_id to DistributorArray
        self._map: WorkflowRunMaps = WorkflowRunMaps()

        # lists of task_instance_ids in different states. used for property views into
        # self._task_instances dict. This gets refreshed from the db during
        # self.get_task_instance_status_updates
        self._registered_task_instance_ids: _tiList = _tiList()
        self._launched_task_instance_ids: _tiList = _tiList()
        self._running_task_instance_ids: _tiList = _tiList()
        self._error_task_instance_ids: _tiList = _tiList()

        # Triaging queue
        # We may not need this; haven't decided yet
        self._triaging_queue = _tiList()

        # flags to mark whether workflow_run completes w/o errors
        self.wfr_completed = False
        self.wfr_has_failed_tis = False

    @property
    def arrays(self) -> List[DistributorArray]:
        """Return a list of arrays."""
        return self._map.get_arrays()

    @property
    def task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of arrays."""
        return self._map.get_task_instances()

    @property
    def registered_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id) for tid in
                self._registered_task_instance_ids.ids]

    @property
    def launched_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id) for tid in
                self._launched_array_task_instance_ids.ids]

    @property
    def running_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of running task_instances"""
        return [DistributorTaskInstance(tid, self.workflow_run_id) for tid in
                self._running_array_task_instance_ids.ids]

    @property
    def registered_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered array task_instances.

        These ids are stored on the array object.
        """
        task_instances: List[DistributorTaskInstance] = []
        for array in self.arrays:
            array_task_instances = [self._task_instances[tiid] for tiid in
                                    array.registered_array_task_instance_ids]
            task_instances.extend(array_task_instances)
        return task_instances

    @property
    def launched_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        task_instances: List[DistributorTaskInstance] = []
        for array in self.arrays:
            array_task_instances = [self._map.get_DistributorTaskInstance_by_id(tiid)
                                    for tiid in array._launched_array_task_instance_ids.ids]
            task_instances.extend(array_task_instances)
        return task_instances

    @property
    def running_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of running task_instances"""
        task_instances: List[DistributorTaskInstance] = []
        for array in self.arrays:
            array_task_instances = [self._map.get_DistributorTaskInstance_by_id(tiid)
                                    for tiid in array._running_array_task_instance_ids.ids]
            task_instances.extend(array_task_instances)
        return task_instances


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
            array = self._map.get_DistributorArray(array_id)
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
            self._map.add_DistributorArray(array)
        return array

    def register_task_instance(self, task: DistributorTask):
        """
        create task instances (task transitions from Queued -> Instantiating)

        attach task instances with Arrays to the associated array object
        add task instances without Arrays to self.registered_task_instances
        """
        # create task instance and add to registry
        task_instance = task.register_task_instance(self.workflow_run_id)
        self._map.add_DistributorTaskInstance(task_instance)

        # if it is an array task queue on the array
        if task.array_id is not None:
            array = self._map.get_DistributorArray(task.array_id)
            array.queue_task_instance_id_for_array_launch(task_instance.task_instance_id)

        # otherwise add to the registered list
        else:
            self._registered_task_instance_ids.add(task_instance.task_instance_id)

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
        ids_to_launch = array.registered_array_task_instance_ids
        array.add_batch_number_to_task_instances()

        # Fetch the command
        command = cluster.build_worker_node_command(task_instance_id=None,
                                                    array_id=array.array_id,
                                                    batch_number=array.batch_number - 1)

        array_distributor_id = cluster.submit_to_batch_distributor(
            command=command,
            name=array.name,  # TODO: array class should have a name in the client model
            requested_resources=array.requested_resources,
            array_length=1  # TODO: integrate with concurrency limit
        )

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

    def _log_workflow_run_heartbeat(self) -> None:
        """Log wfr and tis heartbeat to db and increase next expected time."""
        """TODO: """
        pass

    def _log_tis_heartbeat(self, tis: List) -> None:
        """Log heartbeat of given list of tis."""
        """TODO:"""
        pass

    def refresh_status_from_db(self, list: _tiList, status: str) -> Dict[int: str]:
        """Got to DB to check the list tis status."""
        """TODO: Return a list of tis with status doesn't match status."""
        pass

    def refresh_status_with_distributor(self, list: _tiList, status: str) -> Dict[int, str]:
        """Go to the distributor to check the list tis status."""
        """TODO: Return a list of tis with status doesn't match status."""
        pass


    def heartbeat(self) -> None:
        """Log heartbeats."""
        # log heartbeats for tasks queued for batch execution and for the
        # workflow run
        logger.debug("Distributor: logging heartbeat")
        self._log_workflow_run_heartbeat()  # update wfr hearbeat time in DB
        self._log_tis_heartbeat([ti.task_instance_id for ti in self.task_instances])  # log heartbeat for all tis

        # check launching queue
        # sync with DB
        ti_dict = self.refresh_status_from_db(self._launched_task_instance_ids, "B")
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
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._launched_task_instance_ids, "B", False)
        for tiid in ti_dict.keys():
            if ti_dict[tiid] == "R":
                # do nothing
                pass
            else:
                dwfr._launched_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] == "I":
                    raise Exception("No way this should happen.")
                else:
                    dwfr.wfr_has_failed_tis = True
                    dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

        # check running queue
        # sync with DB
        ti_dict = dwfr.refresh_status_from_db(dwfr._running_task_instance_ids, "R", False)
        for tiid in ti_dict.keys():
            dwfr._running_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("I", "B"):
                raise Exception("No way this should happen.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._running_task_instance_ids, "R", False)
        for tiid in ti_dict.keys():
            dwfr._running_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("B"):
                raise Exception("The cluster much be crazy.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

