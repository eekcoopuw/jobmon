from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Dict, Any, Callable, Tuple, Union

from jobmon.client.task import Task
# from jobmon.client.distributor.distributor_task import DistributorTask
# from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor


"""
Client API
"""


class Array:

    def __init__(
        self,
        task_template_version_id: int,
        max_concurrently_running: int,
        task_args: Dict[str, str],
        op_args: Dict[str, str],
        max_attempts: int = 3,
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        cluster_name: str = "",
    ):
        pass

    def create_task(self, **kwargs) -> None:
        """
        Add a task to this array. node_args are specified as kwargs
        """
        pass

    def get_task_by_node_args(self, **kwargs) -> Task:
        """
        query tasks by node args. Used for setting dependencies
        """
        pass


class TaskTemplate:

    def create_array(
        self,
        max_attempts: int = 3,
        compute_resources: Optional[Dict[str, Any]] = None,
        compute_resources_callable: Optional[Callable] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        cluster_name: str = "",
        node_args: List[Tuple] = None,
        **kwargs: Any,
    ) -> Array:
        """
        Same API as create_task, less the task specific parameters.
        kwargs can be lists of values for node_args. must be single values task args
        and op args

        we will use the cross product of node args for the task expansion.
        alternatively, the user can pass in a list of tuples which will be mapped to node arg
        names in the oder that node args appear in the command string.
        """
        pass


"""
Distributor API
"""


class DistributorTaskInstance:

    def __init__(
        self,
        task_instance_id: int,
        workflow_run_id: int,
        array_id: Optional[int],
        requested_resources: dict,
        requester: Requester,
        status: str,
        status_date: datetime,
    ):
        self.cluster_id = None
        self.distributor_id = None
        self.report_by_date = None

    @classmethod
    def from_wire(
        cls: Any,
        wire_tuple: tuple,
        requester: Requester,
    ) -> DistributorTaskInstance:
        pass

    def transition_to_launched(
        self,
        cluster_id: int,
        distributor_id: int,
        next_report_increment: float,
    ) -> None:
        """Register the submission of a new task instance to a cluster."""
        pass

    def transition_to_no_distributor_id(
        self,
        cluster_id: int,
        no_id_err_msg: str,
    ) -> None:
        """Register that task submission failed with the central service."""
        pass

    def transition_to_unknown_error(self, error_message: str) -> None:
        """Register that an unknown error was discovered during reconciliation."""
        pass

    def transition_to_resource_error(self, error_message: str) -> None:
        """Register that a resource error was discovered during reconciliation."""
        pass

    def transition_to_error(self, error_message: str) -> None:
        """Register that a known error occurred during reconciliation."""
        pass


class DistributorTask:

    def __init__(
        self,
        task_id: int,
        workflow_id: int,
        array_id: Optional[int],
        name: str,
        requester: Requester,
    ):
        pass

    @classmethod
    def from_wire(cls, wire_tuple: tuple, requester: Requester) -> DistributorTask:
        """unpack the response from the server and return a DistributorTask."""
        pass

    def register_task_instance(
        self,
        workflow_run_id: int,
    ) -> DistributorTaskInstance:
        """create a task instance associated with this task instance."""
        pass


class DistributorArrayInstance:

    def __init__(
        self,
        array_instance_id: int,
        task_instance_ids: List[int],
        distributor_id: int,
        cluster_id: int,
    ):
        pass

    @classmethod
    def from_wire(cls):
        pass

    def transition_to_no_distributor_id(self, no_id_err_msg: str) -> None:
        pass

    def transition_to_launched(
        self,
        cluster_id: int,
        distributor_id: int,
        next_report_increment: float,
    ) -> None:
        pass


class DistributorArray:

    def __init__(self, array_id, max_concurrently_running: int):
        self._registered_array_task_instance_ids: List[int] = []

    @classmethod
    def from_wire(cls, wire_tuple: tuple, requester: Requester) -> DistributorTask:
        pass

    @classmethod
    def register_array_instance(cls, requester: Requester) -> DistributorArrayInstance:
        """
        register array instance with db here.
        """
        pass

    def queue_task_instance_id_for_array_launch(self, task_instance_id: int):
        """
        Add task instance to array queue
        """
        pass


class DistributorWorkflowRun:

    """
    This class is responsible for implementing workflow level bulk routes and tracking in
    memory state on the distributor.

    when polling from the database we should work in task space and translate into array
    space in memory where appropriate.

    when pushing to the database we should work in CommandType (Workflow/Array/Task) space
    """

    def __init__(self, workflow_id: int, workflow_run_id: int):
        # mapping of task_instance_id to DistributorTaskInstance
        self._task_instances: Dict[int, DistributorTaskInstance] = {}

        # lists of task_instance_ids in different states. used for property views into
        # self._task_instances dict. This gets refreshed from the db during
        # self.get_task_instance_status_updates
        self._registered_task_instance_ids: List[int] = []
        self._launched_task_instance_ids: List[int] = []
        self._running_task_instance_ids: List[int] = []

        # mapping of array_id to DistributorArray. stores the queue of task_instances to be
        # instantiated using the array strategy.
        self._arrays: Dict[int, DistributorArray] = {}
        self._launched_array_task_instance_ids: List[int] = []
        self._running_array_task_instance_ids: List[int] = []

        self.accounting_queue: List[DistributorTaskInstance] = []
        self.launching_errors_queue: List[DistributorTaskInstance] = []

    @property
    def arrays(self) -> List[DistributorArray]:
        """Return a list of arrays"""
        pass

    @property
    def registered_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered task_instances"""
        pass

    @property
    def launched_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    @property
    def running_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    @property
    def registered_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of registered array task_instances.

        These ids are stored on the array object.
        """
        pass

    @property
    def launched_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    @property
    def running_array_task_instances(self) -> List[DistributorTaskInstance]:
        """Return a list of launched task_instances"""
        pass

    def instantiate_queued_tasks(self) -> None:
        """
        # aka get new work to be done.

        get list of tasks to create task_instances for (QUEUED Tasks)

        loop through all queued tasks
        create task instances (task transitions from Queued -> Instantiating)

        attach task instances with Arrays to the associated array object
        add task instances without Arrays to self.registered_task_instances

        loop through arrays and see if we can create any array instances add array instances to
        self.registered_array_instances
        """
        pass

    def _get_tasks_queued_for_instantiation(self) -> List[DistributorTask]:
        """Retrieve a list of task that are in queued state"""
        pass

    def refresh_task_instances_from_db(self) -> None:
        """
        get task instance FSM updates.
        refresh FSM lists with any new states.
        """
        pass

    def refresh_arrays_from_db(self) -> None:
        """compute num done in array"""
        pass

    def refresh_heartbeats_from_distributor(self) -> None:
        """
        Retrieve a list of task instances that haven't logged heartbeats recently

        Adds values to self.reconciliation_queue
        """
        pass

    def get_submitted_or_running(self) -> List[DistributorTaskInstance]:
        """This method is used when resume is set or keyboard interrupt is used

        do we neet to translate back from task instances into array tasks?
        """
        pass

    def get_launched_task_instances(self, cluster_name: str) -> List[int]:
        """
        Question: sync from the db in this method?

        return the distributor_ids of the tasks that the this deployment unit thinks are
        submitted or running.
        """
        pass

    def get_launched_array_task_instances(
        self,
        cluster_name: str
    ) -> List[Tuple[int, int]]:
        """
        Question: sync from the db in this method?

        return the distributor_id/task_id tuple of the tasks that the this deployment unit
        thinks are submitted or running.
        """
        pass

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
        array_instance: DistributorArrayInstance,
        cluster: ClusterDistributor
    ):
        """
        submits an array task on a given distributor
        adds the new task instances to self.submitted_or_running_array_task_instances
        """
        pass

    def account_for_task_instance_error(
        self,
        task_instance: DistributorTaskInstance,
        cluster: ClusterDistributor
    ):
        """
        Check qacct for the given distributor_id (task_instances)
        or distributor_id/task_id (array task_instance). Infer an error and log it.
        """
        pass

    def log_task_instance_heartbeats(self, distributor_ids: List[int]):
        """logs heartbeat for task instances

        distributor_ids: a list of distributor_ids for queued or running task instances

        """
        pass

    def log_array_instance_heartbeats(self, distributor_ids: List[Tuple[int, int]]):
        """logs heartbeat for task instances associated with array instances

        distributor_ids: a list of tuples where the first element in the tuple is the
            distributor_id of the array task instance and the second element is the task_id.
        """
        pass

    def log_workflow_run_heartbeat(self) -> None:
        """logs heartbeat for this workflow run"""
        pass


class DistributorService:

    """
    This class has a bidirectional flow of information.

    It polls from the database and pushes new requests onto clusters.

    It pushes heartbeats from the cluster into the database.

    Is responsible for coordinating between instances and the correct cluster.


    """

    def __init__(
        self,
        workflow_id: int,
        workflow_run_id: int,
        distributor: ClusterDistributor
    ):
        self.distributor = distributor
        self.workflow_run = DistributorWorkflowRun(workflow_id, workflow_run_id)

    def instantiate_tasks(self):
        """
        pulls work from the db and pushes it to the distributor
        """
        self.workflow_run.instantiate_queued_tasks()  # may need split to ensure heartbeats

        for task_instance in self.workflow_run.registered_task_instances:
            self.workflow_run.launch_task_instance(task_instance, self.distributor)

        while self.workflow_run.registered_array_instances:
            array_instance = self.workflow_run.registered_array_instances.pop(0)
            # if we had multiple clusters the reference to distributor would be a lookup
            self.workflow_run.launch_task_instance(array_instance, self.distributor)

    def triage_arrested_tasks(self):
        """
        populates the accounting queue by polling from the db and checking against distributor
        accounting. effectively a triaging loop for tasks but we don't have that state in the
        FSM currently.
        """
        # reconciliation between lost heartbeats and qacct/sacct

        # here we could move jobs into triaging due to missed heartbeats
        self.workflow_run.get_suspicious_task_instances()
        while self.workflow_run.accounting_queue:
            task_instance = self.workflow_run.accounting_queue.pop(0)
            self.workflow_run.account_for_task_instance_error(task_instance, self.distributor)

    def triage_launching_errors(self):
        """
        populates batch submission queue by polling from the db and checking against the
        distributor for tasks in eqw.

        if in eqw or equivalent, terminate and log error
        """

        task_instances = self.workflow_run.launched_task_instances(
            self.distributor.cluster_name
        )
        task_instance_distributor_id_mapping = {
            task_instance.distributor_id: task_instance for task_instance in task_instances
        }
        distributor_ids = list(task_instance_distributor_id_mapping.keys())

        distributor_errors = self.distributor.get_queueing_errors(distributor_ids)
        if distributor_errors:
            distributor_error_ids = list(distributor_errors.keys())
            self.distributor.terminate_task_instances(list(distributor_error_ids.keys()))

            # store error message and handle in distributing thread
            for distributor_id, msg in distributor_errors.items():
                task_instance = task_instance_distributor_id_mapping[distributor_id]
                task_instance.error_state = TaskInstanceStatus.ERROR_FATAL
                task_instance.error_msg = msg
                task_instance.log_error()
