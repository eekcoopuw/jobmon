from __future__ import annotations

from datetime import datetime
import logging
import signal
import sys
import time
from typing import Dict, List, Optional, Set

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_array_batch import DistributorArrayBatch
from jobmon.client.distributor.distributor_command import DistributorCommand
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow import DistributorWorkflow
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester


logger = logging.getLogger(__name__)


class DistributorService:
    def __init__(
        self,
        cluster: ClusterDistributor,
        requester: Requester,
        workflow_run_heartbeat_interval: int = 30,
        task_instance_heartbeat_interval: int = 90,
        heartbeat_report_by_buffer: float = 3.1,
        distributor_poll_interval: int = 10,
        worker_node_entry_point: Optional[str] = None,
        raise_on_error: bool = False
    ) -> None:

        # operational args
        self._worker_node_entry_point = worker_node_entry_point
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self._distributor_poll_interval = distributor_poll_interval
        self.raise_on_error = raise_on_error

        # interrupt signal
        self._signal_recieved = False

        # indexing of task instance by associated id
        self._task_instances: Dict[int, DistributorTaskInstance] = {}
        self._tasks: Dict[int, DistributorTask] = {}
        self._arrays: Dict[int, DistributorArray] = {}
        self._workflows: Dict[int, DistributorWorkflow] = {}

        # work queue
        self.distributor_commands: List[DistributorCommand] = []

        # indexing of task instanes by status
        self._task_instance_status_map: Dict[str, Set[DistributorTaskInstance]] = {
            TaskInstanceStatus.QUEUED: set(),
            TaskInstanceStatus.INSTANTIATED: set(),
            TaskInstanceStatus.LAUNCHED: set(),
            TaskInstanceStatus.RUNNING: set(),
            TaskInstanceStatus.TRIAGING: set(),
            TaskInstanceStatus.KILL_SELF: set(),
        }
        # order through which we processes work
        self._status_processing_order = [
            TaskInstanceStatus.QUEUED,
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
            TaskInstanceStatus.TRIAGING,
            TaskInstanceStatus.KILL_SELF,
        ]
        self._command_generator_map = {
            TaskInstanceStatus.QUEUED: self._check_queued_for_work,
            TaskInstanceStatus.INSTANTIATED: self._check_instantiated_for_work,
            TaskInstanceStatus.LAUNCHED: self._check_launched_for_work,
            TaskInstanceStatus.TRIAGING: self._check_triaging_for_work,
            TaskInstanceStatus.KILL_SELF: self._check_kill_self_for_work,
        }

        # syncronization timings
        self._last_heartbeat_time = time.time()

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
        self.workflow_run.transition_to_instantiated()

    def run(self):
        # start the cluster
        try:
            self._initialize_signal_handlers()
            self.cluster.start()
            self.workflow_run.transition_to_launched()

            # signal via pipe that we are alive
            sys.stderr.write("ALIVE")
            sys.stderr.flush()

            # process commands forever
            while not self._signal_recieved:

                loop_start = time.time()

                # process the next status
                status = self._status_processing_order.pop(0)
                self.process_status(status)
                self._status_processing_order.append(status)

                # log how long the loop took and take a break if needed
                loop_duration = time.time() - loop_start
                logger.info(
                    f"Status processing loop for status={status} took {int(loop_duration)}s."
                )
                if loop_duration < self._distributor_poll_interval:
                    time.sleep(self._distributor_poll_interval - loop_duration)

        finally:
            # stop distributor
            self.cluster.stop()

            # signal via pipe that we are shutdown
            sys.stderr.write("SHUTDOWN")
            sys.stderr.flush()

    def process_status(self, status: str):
        """"""
        try:

            # syncronize statuses from the db and get new work
            self._check_for_work(status)

            while self.distributor_commands and not self._signal_recieved:
                # check if we need to pause for a heartbeat
                time_diff = time.time() - self._last_heartbeat_time
                if time_diff > self._workflow_run_heartbeat_interval:
                    self.heartbeat()

                # get the first callable and run it. log any errors
                distributor_command = self.distributor_commands.pop(0)
                distributor_command(self.raise_on_error)
                if distributor_command.error_raised:
                    logger.error(distributor_command.exception)

        finally:
            # update task mappings
            self._update_status_map(status)

            # make sure distributor_commands is empty
            self.distributor_commands = []

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
            batch_number=array_batch.batch_number,
        )
        try:
            # submit array to distributor
            distributor_id_map = self.cluster.submit_array_to_batch_distributor(
                command=command,
                name=array_batch.name,
                requested_resources=array_batch.requested_resources,
                array_length=len(array_batch.task_instances),
            )

        except NotImplementedError:
            # create DistributorCommands to submit the launch if array isn't implemented
            for task_instance in array_batch.task_instances:
                distributor_command = DistributorCommand(
                    self.launch_task_instance,
                    task_instance,
                    "TODO:GETNAME",
                )
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
                distributor_id = distributor_id_map[task_instance.array_step_id]
                distributor_command = DistributorCommand(
                    task_instance.transition_to_launched, distributor_id,
                    self._next_report_increment
                )
                self.distributor_commands.append(distributor_command)

    def launch_task_instance(self, task_instance: DistributorTaskInstance, name: str) -> None:
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
            distributor_id = self.cluster.submit_to_batch_distributor(
                command=command,
                name=name,
                requested_resources=task_instance.requested_resources
            )

        except Exception as e:
            task_instance.transition_to_no_distributor_id(no_id_err_msg=str(e))

        else:
            # move from register queue to launch queue
            task_instance.transition_to_launched(distributor_id, self._next_report_increment)

    def triage_error(self, task_instance: DistributorTaskInstance) -> None:
        r_value, r_msg = self.cluster.get_remote_exit_info(task_instance.distributor_id)
        task_instance.transition_to_error(r_msg, r_value)

    def kill_self(self, task_instance: DistributorTaskInstance) -> None:
        self.cluster.terminate_task_instances([task_instance.distributor_id])
        task_instance.transition_to_error("Task instance was self-killed.", TaskInstanceStatus.ERROR)

    def log_task_instance_report_by_date(self) -> None:
        task_instances_launched = self._task_instance_status_map[TaskInstanceStatus.LAUNCHED]

        submitted_or_running = self.cluster.get_submitted_or_running(
            [x.distributor_id for x in task_instances_launched]
        )

        task_instance_ids_to_heartbeat: List[int] = []
        for task_instance_launched in task_instances_launched:
            if task_instance_launched.distributor_id in submitted_or_running:
                task_instance_ids_to_heartbeat.append(task_instance_launched.task_instance_id)

        """Log the heartbeat to show that the task instance is still alive."""
        logger.debug(f"Logging heartbeat for task_instance {task_instance_ids_to_heartbeat}")
        message: Dict = {"next_report_increment": self._next_report_increment,
                         "task_instance_ids": task_instance_ids_to_heartbeat}
        rc, _ = self.requester.send_request(
            app_route="/task_instance/log_report_by/batch",
            message=message,
            request_type="post",
            logger=logger,
        )

    def _initialize_signal_handlers(self):
        def handle_sighup(signal, frame):
            logging.info("received SIGHUP")
            self._signal_recieved = True

        def handle_sigterm(signal, frame):
            logging.info("received SIGTERM")
            self._signal_recieved = True

        def handle_sigint(signal, frame):
            logging.info("received SIGINT")
            self._signal_recieved = True

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGHUP, handle_sighup)
        signal.signal(signal.SIGINT, handle_sigint)

    def _check_for_work(self, status: str):
        """Got to DB to check the list tis status."""
        message = {
            "task_instance_ids": [
                task_instance.task_instance_id
                for task_instance in self._task_instance_status_map[status]
            ],
            "status": status,
        }
        app_route = f"/workflow_run/{self.workflow_run.workflow_run_id}/sync_status"
        return_code, result = self.requester.send_request(
            app_route=app_route, message=message, request_type="post"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"{app_route} Returned={return_code}. Message={message}"
            )

        # mutate the statuses and update the status map
        status_updates: Dict[str, str] = result["status_updates"]
        for task_instance_id_str, status in status_updates.items():
            task_instance_id = int(task_instance_id_str)
            try:
                task_instance = self._task_instances[task_instance_id]

            except KeyError:
                task_instance = DistributorTaskInstance(
                    task_instance_id,
                    self.workflow_run.workflow_run_id,
                    status,
                    self.requester,
                )
                self._task_instance_status_map[task_instance.status].add(task_instance)

            else:
                # remove from old status set
                previous_status = task_instance.status
                self._task_instance_status_map[previous_status].remove(task_instance)

                # change to new status and move to new set
                task_instance.status = status

                try:
                    self._task_instance_status_map[task_instance.status].add(task_instance)
                except KeyError:
                    # If the task instance is in a terminal state, e.g. D, E, etc.,
                    # expire it from the distributor
                    continue

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
        queued_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.QUEUED
        ]
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

        # store arrays and eligible task_instances for later
        arrays: Set[DistributorArray] = set()
        eligible_task_instances: Set[DistributorTaskInstance] = set()

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

            # add to eligible_task_instances set if there is capacity
            if workflow_capacity > 0 and array_capacity > 0:
                eligible_task_instances.add(task_instance)

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
            array_batches = array.create_array_batches(eligible_task_instances)

            for array_batch in array_batches:
                distributor_command = DistributorCommand(
                    self.launch_array_batch, array_batch
                )
                self.distributor_commands.append(distributor_command)

    def _check_launched_for_work(self) -> None:
        # no actual work for launched
        pass

    def _check_triaging_for_work(self) -> None:
        """For TaskInstances with TRIAGING status, check the nature of no heartbeat,
        and change the statuses accordingly"""
        triaging_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.TRIAGING
        ]
        for task_instance in triaging_task_instances:
            distributor_command = DistributorCommand(self.triage_error, task_instance)
            self.distributor_commands.append(distributor_command)

    def _check_kill_self_for_work(self) -> None:
        """For TaskInstances with KILL_SELF status, terminate it and
        transition it to error accordingly"""

        kill_self_task_instances = self._task_instance_status_map[
            TaskInstanceStatus.KILL_SELF
        ]

        for task_instance in kill_self_task_instances:
            distributor_command = DistributorCommand(self.kill_self, task_instance)
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
            workflow = self._workflows[workflow_id]
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

    def heartbeat(self):
        pass