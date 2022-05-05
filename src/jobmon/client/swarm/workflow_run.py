"""Workflow Run is an distributor instance of a declared workflow."""
from __future__ import annotations

from datetime import datetime
import logging
import time
from typing import Callable, Dict, Generator, List, Optional, Set, TYPE_CHECKING, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.swarm.swarm_array import SwarmArray
from jobmon.client.task_resources import TaskResources
from jobmon.constants import TaskStatus, WorkflowRunStatus, TaskResourcesType
from jobmon.exceptions import (
    CallableReturnedInvalidObject,
    DistributorNotAlive,
    InvalidResponse,
    WorkflowTestError,
    TransitionError,
)

from jobmon.requester import http_request_ok, Requester

# avoid circular imports on backrefs
if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow


logger = logging.getLogger(__name__)


class SwarmCommand:
    def __init__(self, func: Callable[..., None], *args, **kwargs):
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state
            *args: positional args to be passed into func
            **kwargs: kwargs to be to be passed into func
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        self._func(*self._args, **self._kwargs)


class WorkflowRun:
    """WorkflowRun enables tracking for multiple runs of a single Workflow.

    A Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(
        self,
        workflow_run_id: int,
        workflow_run_heartbeat_interval: int = 30,
        heartbeat_report_by_buffer: float = 3.1,
        fail_fast: bool = False,
        wedged_workflow_sync_interval: int = 600,
        fail_after_n_executions: int = 1_000_000_000,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialization of the swarm WorkflowRun."""
        self.workflow_run_id = workflow_run_id

        # state tracking
        self._active_states = [
            TaskStatus.QUEUED,
            TaskStatus.INSTANTIATING,
            TaskStatus.LAUNCHED,
            TaskStatus.RUNNING,
        ]
        self.tasks: Dict[int, SwarmTask] = {}
        self.arrays: Dict[int, SwarmArray] = {}
        self.ready_to_run: List[SwarmTask] = []
        self._task_status_map: Dict[str, Set[SwarmTask]] = {
            TaskStatus.REGISTERING: set(),
            TaskStatus.QUEUED: set(),
            TaskStatus.INSTANTIATING: set(),
            TaskStatus.LAUNCHED: set(),
            TaskStatus.RUNNING: set(),
            TaskStatus.DONE: set(),
            TaskStatus.ADJUSTING_RESOURCES: set(),
            TaskStatus.ERROR_FATAL: set(),
        }

        # cache to get same id
        self._task_resources: Dict[int, TaskResources] = {}

        # workflow run attributes
        self._status = WorkflowRunStatus.BOUND
        self._last_heartbeat_time = time.time()

        # flow control
        self.fail_fast = fail_fast
        self.wedged_workflow_sync_interval = wedged_workflow_sync_interval

        # test parameters to force failure
        self._val_fail_after_n_executions = fail_after_n_executions
        self._n_executions = 0

        # optional config
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        if requester is None:
            requester = Requester(ClientConfig.from_defaults().url)
        self._requester = requester

        # This signal is set if the workflow run receives a resume
        self._terminated = False

    @property
    def status(self) -> str:
        """Status of the workflow run."""
        return self._status

    @property
    def done_tasks(self) -> List[SwarmTask]:
        return list(self._task_status_map[TaskStatus.DONE])

    @property
    def failed_tasks(self) -> List[SwarmTask]:
        return list(self._task_status_map[TaskStatus.ERROR_FATAL])

    @property
    def active_tasks(self) -> bool:
        """Based on the task status map, does the workflow run have more work or not.

        If there are no tasks in active states, the fringe is empty and
        therefore we should error out.
        """

        # To prevent additional compute, return False immediately if set to an error state.
        # Likely done by fail fast or the max execution loops
        if self.status in (WorkflowRunStatus.ERROR, WorkflowRunStatus.TERMINATED):
            return False

        any_active_tasks = any(
            [any(self._task_status_map[s]) for s in self._active_states]
        ) or any(self.ready_to_run)
        return any_active_tasks

    def from_workflow(self, workflow: Workflow) -> None:
        self.workflow_id = workflow.workflow_id
        self.max_concurrently_running: int = workflow.max_concurrently_running

        # construct arrays
        for array in workflow.arrays.values():
            swarm_array = SwarmArray(array.array_id, array.max_concurrently_running)
            self.arrays[array.array_id] = swarm_array

        # construct SwarmTasks from Client Tasks and populate registry
        for task in workflow.tasks.values():

            cluster = workflow.get_cluster_by_name(task.cluster_name)
            fallback_queues = []
            for queue in task.fallback_queues:
                cluster_queue = cluster.get_queue(queue)
                fallback_queues.append(cluster_queue)

            # create swarmtasks
            swarm_task = SwarmTask(
                task_id=task.task_id,
                array_id=task.array.array_id,
                status=task.initial_status,
                max_attempts=task.max_attempts,
                cluster=cluster,
                task_resources=task.original_task_resources,
                compute_resources_callable=task.compute_resources_callable,
                resource_scales=task.resource_scales,
                fallback_queues=fallback_queues,
            )
            self.tasks[task.task_id] = swarm_task

        # create relationships on swarm task
        for task in workflow.tasks.values():
            swarm_task = self.tasks[task.task_id]

            # assign upstream and downstreams
            swarm_task.upstream_swarm_tasks = set(
                [self.tasks[t.task_id] for t in task.upstream_tasks]
            )
            swarm_task.downstream_swarm_tasks = set(
                [self.tasks[t.task_id] for t in task.downstream_tasks]
            )

            # create array association
            self.arrays[swarm_task.array_id].add_task(swarm_task)

            # assign each task to the correct set
            self._task_status_map[swarm_task.status].add(swarm_task)

            # compute initial fringe
            if swarm_task.status == TaskStatus.DONE:
                # if task is done check if there are downstreams that can run
                for downstream in swarm_task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1

        self.last_sync = self._get_current_time()
        self.num_previously_complete = len(self._task_status_map[TaskStatus.DONE])

    def run(
        self,
        distributor_alive_callable: Callable[..., bool],
        seconds_until_timeout: int = 36000,
        initialize: bool = True
    ):
        """Take a concrete DAG and queue al the Tasks that are not DONE.

        Uses forward chaining from initial fringe, hence out-of-date is not
        applied transitively backwards through the graph. It could also use
        backward chaining from an identified goal node, the effect is
        identical.

        Conceptually:
        all tasks in registering state w/ finished upstreams are ready_to_run
        Put tasks in Adjusting state on the ready_to_run queue

        while there are tasks ready_to_run or currently running tasks:
            queue all tasks that are ready_to_run
            wait for some jobs to complete and add downstreams to the ready_to_run queue
            rinse and repeat

        Args:
            swarm: the workflow run associated with the swarm.
            fail_fast: raise error on the first failed task.
            seconds_until_timeout: how long to block while waiting for the next task to finish
                before raising an error.
            wedged_workflow_sync_interval: the time interval to sync a workflow that is wedged.

        Return:
            workflow_run status
        """
        try:
            if initialize:
                logger.info(f"Executing Workflow Run {self.workflow_run_id}")
                self._update_status(WorkflowRunStatus.RUNNING)
                self.set_initial_fringe()

            time_since_last_full_sync = 0.0
            total_elapsed_time = 0.0
            terminating_states = [
                WorkflowRunStatus.COLD_RESUME,
                WorkflowRunStatus.HOT_RESUME,
            ]

            while self.active_tasks:

                # Expire the swarm after the requested number of seconds
                if total_elapsed_time > seconds_until_timeout:
                    raise RuntimeError(
                        f"Not all tasks completed within the given workflow timeout length "
                        f"({seconds_until_timeout} seconds). Submitted tasks will still run, "
                        f"but the workflow will need to be restarted."
                    )

                # check that the distributor is still alive
                if not distributor_alive_callable():
                    raise DistributorNotAlive(
                        "Distributor process unexpectedly stopped. Workflow will error."
                    )

                # If the workflow run status was updated asynchronously, terminate
                # all active task instances and error out.
                if self.status in terminating_states:
                    logger.warning(
                        f"Workflow Run set to {self.status}. Attempting graceful shutdown."
                    )
                    # Active task instances will be set to "K", the processing loop then
                    # keeps running until all of the states are appropriately set.
                    self._terminate_task_instances()

                # if fail fast and any error
                if self.fail_fast and self._task_status_map[TaskStatus.ERROR_FATAL]:
                    logger.info("Failing after first failure, as requested")
                    break

                # fail during test path
                if self._n_executions >= self._val_fail_after_n_executions:
                    raise WorkflowTestError(
                        f"WorkflowRun asked to fail after {self._n_executions} "
                        "executions. Failing now"
                    )

                # process any commands that we can in the time allotted
                loop_start = time.time()
                time_till_next_heartbeat = self._workflow_run_heartbeat_interval - (
                    loop_start - self._last_heartbeat_time
                )
                if self.status == WorkflowRunStatus.RUNNING:
                    self.process_commands(timeout=time_till_next_heartbeat)

                # take a break if needed
                loop_elapsed = time.time() - loop_start
                if loop_elapsed < time_till_next_heartbeat:
                    sleep_time = time_till_next_heartbeat - loop_elapsed
                    time.sleep(sleep_time)
                    loop_elapsed += sleep_time

                # then synchronize state
                if time_since_last_full_sync > self.wedged_workflow_sync_interval:
                    time_since_last_full_sync = 0.0
                    self.synchronize_state(full_sync=True)
                else:
                    time_since_last_full_sync += loop_elapsed
                    self.synchronize_state()

                total_elapsed_time += time.time() - loop_start

        # user interrupt
        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt raised")
            confirm = input("Are you sure you want to exit (y/n): ")
            confirm = confirm.lower().strip()

            if confirm == "y":
                self._update_status(WorkflowRunStatus.STOPPED)
                raise
            else:
                logger.info("Continuing jobmon...")
                seconds_until_timeout = int(seconds_until_timeout - loop_elapsed)
                self.run(distributor_alive_callable, seconds_until_timeout, initialize=False)

        # unexpected errors. raise
        except Exception as e:
            try:
                self._update_status(WorkflowRunStatus.ERROR)
            except TransitionError as trans:
                logger.warning(trans)
            raise e

        # no more active tasks
        else:
            # check if done
            if len(self.tasks) == len(self._task_status_map[TaskStatus.DONE]):
                logger.info("All tasks are done")
                self._update_status(WorkflowRunStatus.DONE)

            else:
                if self.status in terminating_states:
                    self._update_status(WorkflowRunStatus.TERMINATED)
                else:
                    self._update_status(WorkflowRunStatus.ERROR)

    def set_initial_fringe(self):
        """set initial fringe"""
        for t in [t for t in self._task_status_map[TaskStatus.ADJUSTING_RESOURCES]]:
            self._set_adjusted_task_resources(t)
            self.ready_to_run.append(t)

        for t in [
            task
            for task in self._task_status_map[TaskStatus.REGISTERING]
            if task.all_upstreams_done
        ]:
            self._set_validated_task_resources(t)
            self.ready_to_run.append(t)

    def get_swarm_commands(self) -> Generator[SwarmCommand, None, None]:
        """Generator to get next chunk of work to be done. Must be idempotent"""

        # compute capacities. max - active
        active_tasks: Set[SwarmTask] = set()
        for task_status in self._active_states:
            active_tasks = active_tasks.union(self._task_status_map[task_status])
        workflow_capacity = self.max_concurrently_running - len(active_tasks)
        array_capacity_lookup: Dict[int, int] = {
            aid: array.max_concurrently_running - len(active_tasks.intersection(array.tasks))
            for aid, array in self.arrays.items()
        }

        try:
            unscheduled_tasks: List[SwarmTask] = []
            while self.ready_to_run and workflow_capacity > 0:
                # pop the next task off of the queue
                next_task = self.ready_to_run.pop(0)
                array_id = next_task.array_id
                task_resources = next_task.current_task_resources

                # check capacity. add to current batch if room.
                current_batch: List[SwarmTask] = []
                array_capacity = array_capacity_lookup[array_id]
                if array_capacity > 0:
                    current_batch.append(next_task)
                    workflow_capacity -= 1
                    array_capacity -= 1

                    # we started a batch. let's try and add compatible tasks
                    compatible_indices: List[int] = []
                    for index, task in enumerate(self.ready_to_run):

                        # check for batch compatible tasks
                        if (
                            workflow_capacity > 0
                            and array_capacity > 0
                            and task.array_id == array_id
                            and task.current_task_resources == task_resources
                        ):
                            current_batch.append(task)
                            compatible_indices.append(index)
                            workflow_capacity -= 1
                            array_capacity -= 1

                    # remove from original queue in reverse order so the indices don't move
                    compatible_indices.reverse()
                    for index in compatible_indices:
                        task = self.ready_to_run.pop(index)

                    # set final array capacity
                    array_capacity_lookup[array_id] = array_capacity

                    yield SwarmCommand(self.queue_task_batch, current_batch)

                # no room. keep track for next time method is called
                else:
                    unscheduled_tasks.append(next_task)

        # make sure to put unscheduled back on queue, even when the generator is closed
        finally:
            self.ready_to_run = unscheduled_tasks + self.ready_to_run

    def process_commands(self, timeout: Union[int, float] = -1):
        """Processes swarm commands until all work is done or timeout is reached.

        Args:
            timeout: time until we stop processing. -1 means process till no more work
        """

        swarm_commands = self.get_swarm_commands()

        # this way we always process at least 1 command
        loop_start = time.time()
        keep_processing = True
        while keep_processing:

            # run commands
            try:

                # use an iterator so we don't waste compute
                swarm_command = next(swarm_commands)
                swarm_command()

                # if we need a status sync close the generator. next will raise StopIteration
                if not ((time.time() - loop_start) < timeout or timeout == -1):
                    swarm_commands.close()

            except StopIteration:
                # stop processing commands if we are out of commands
                keep_processing = False

    def synchronize_state(self, full_sync: bool = False) -> None:
        self._set_status_for_triaging()
        self._log_heartbeat()
        self._task_status_updates(full_sync=full_sync)

    def _refresh_task_status_map(self, updated_tasks: Set[SwarmTask]) -> None:
        # remove these tasks from old mapping
        for status in self._task_status_map.keys():
            self._task_status_map[status] = (
                self._task_status_map[status] - updated_tasks
            )

        num_newly_completed = 0
        num_newly_failed = 0
        for task in updated_tasks:

            # assign each task to the correct set
            self._task_status_map[task.status].add(task)

            if task.status == TaskStatus.DONE:
                num_newly_completed += 1
                self._n_executions += 1  # a test param

                # if task is done check if there are downstreams that can run
                for downstream in task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1
                    if downstream.all_upstreams_done:
                        self._set_validated_task_resources(downstream)
                        self.ready_to_run.append(downstream)

            elif task.status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            elif task.status == TaskStatus.REGISTERING and task.all_upstreams_done:
                self._set_validated_task_resources(task)
                self.ready_to_run.append(task)

            elif task.status == TaskStatus.ADJUSTING_RESOURCES:
                self._set_adjusted_task_resources(task)

                # put at front of queue since we already tried it once
                self.ready_to_run = [task] + self.ready_to_run

            else:
                logger.debug(
                    f"Got status update {task.status} for task_id: {task.task_id}."
                    "No actions necessary."
                )

        # if newly done report percent done and check if all done
        if num_newly_completed > 0:
            percent_done = round(
                (len(self._task_status_map[TaskStatus.DONE]) / len(self.tasks)) * 100, 2
            )
            logger.info(
                f"{num_newly_completed} newly completed tasks. {percent_done} percent done."
            )

        # if newly failed, report failures and check if we should error out
        if num_newly_failed > 0:
            logger.warning(f"{num_newly_failed} newly failed tasks.")

    def _set_status_for_triaging(self):
        app_route = f"/workflow_run/{self.workflow_run_id}/set_status_for_triaging"
        return_code, response = self._requester.send_request(
            app_route=app_route, message={}, request_type="post"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

    def _log_heartbeat(self):
        next_report_increment = (
            self._workflow_run_heartbeat_interval * self._heartbeat_report_by_buffer
        )
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={
                "status": self._status,
                "next_report_increment": next_report_increment,
            },
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        self._last_heartbeat_time = time.time()

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""

        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        if self.status != status:
            raise TransitionError(
                f"Cannot transition WFR {self.workflow_run_id} from current status "
                f"{self._status} to {status}."
            )

    def _terminate_task_instances(self) -> None:
        """Terminate the workflow run."""
        app_route = f"/workflow_run/{self.workflow_run_id}/terminate_task_instances"
        return_code, response = self._requester.send_request(
            app_route=app_route, message={}, request_type="put"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._terminated = True

    def _set_fail_after_n_executions(self, n: int) -> None:
        """For use during testing.

        Force the TaskDag to 'fall over' after n executions, so that the resume case can be
        tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self._val_fail_after_n_executions = n

    def _get_current_time(self) -> datetime:
        app_route = "/time"
        return_code, response = self._requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        return response["time"]

    def _task_status_updates(self, full_sync: bool = False) -> None:
        """Update internal state of tasks to match the database.

        If no tasks are specified, get all tasks.
        """
        if full_sync:
            message = {}
        else:
            message = {"last_sync": str(self.last_sync)}

        app_route = f"/workflow/{self.workflow_id}/task_status_updates"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self.last_sync = response["time"]

        new_status_tasks: Set[SwarmTask] = set()
        for current_status, task_ids in response["tasks_by_status"].items():
            for task_id in task_ids:
                task = self.tasks[task_id]
                if current_status != task.status:
                    task.status = current_status
                    new_status_tasks.add(task)
        self._refresh_task_status_map(new_status_tasks)

    def queue_task_batch(self, tasks: List[SwarmTask]) -> None:
        first_task = tasks[0]
        task_resources = first_task.current_task_resources
        if not task_resources.is_bound:
            task_resources.bind()

        app_route = f"/array/{first_task.array_id}/queue_task_batch"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={
                "task_ids": [task.task_id for task in tasks],
                "task_resources_id": task_resources.id,
                "workflow_run_id": self.workflow_run_id,
                "cluster_id": first_task.cluster.id,
            },
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        updated_tasks = set()
        for status, task_ids in response["tasks_by_status"].items():
            for task_id in task_ids:
                task = self.tasks[task_id]
                task.status = status
                updated_tasks.add(task)
        self._refresh_task_status_map(updated_tasks)

    def _set_validated_task_resources(self, task: SwarmTask) -> None:
        # get cluster and original params
        cluster = task.cluster
        resource_params = (
            task.current_task_resources.concrete_resources.resources.copy()
        )
        queue = task.current_task_resources.concrete_resources.queue

        # update with extra params
        if task.compute_resources_callable is not None:
            dynamic_compute_resources = task.compute_resources_callable()
            if not isinstance(dynamic_compute_resources, dict):
                raise CallableReturnedInvalidObject(
                    f"compute_resources_callable={task.compute_resources_callable} for "
                    f"task_id={task.task_id} returned an invalid type. Must return dict. got "
                    f"{type(dynamic_compute_resources)}."
                )
            resource_params.update(dynamic_compute_resources)
            task.compute_resources_callable = None

        (
            _,
            _,
            concrete_resource,
        ) = cluster.concrete_resource_class.validate_and_create_concrete_resource(
            queue, resource_params
        )

        # if validated concrete resources are different than original. get new resource object
        validated_resource_hash = hash(concrete_resource)
        if validated_resource_hash != hash(task.current_task_resources.concrete_resources):
            try:
                task_resources = self._task_resources[validated_resource_hash]
            except KeyError:
                task_resources = TaskResources(
                    concrete_resources=concrete_resource,
                    task_resources_type_id=TaskResourcesType.VALIDATED,
                )
                self._task_resources[validated_resource_hash] = task_resources
        else:
            task_resources = task.current_task_resources
            self._task_resources[validated_resource_hash] = task_resources
        task.current_task_resources = task_resources

    def _set_adjusted_task_resources(self, task: SwarmTask) -> None:
        """Adjust the swarm task's parameters.

        Use the cluster API to generate the new resources, then bind to input swarmtask.
        """
        # current resources
        resource_params = (
            task.current_task_resources.concrete_resources.resources.copy()
        )

        concrete_resource = (
            task.cluster.concrete_resource_class.adjust_and_create_concrete_resource(
                existing_resources=resource_params,
                resource_scales=task.resource_scales,
                expected_queue=task.current_task_resources.queue,
                fallback_queues=task.fallback_queues,
            )
        )

        try:
            task_resources = self._task_resources[hash(concrete_resource)]
        except KeyError:
            task_resources = TaskResources(
                concrete_resources=concrete_resource,
                task_resources_type_id=TaskResourcesType.ADJUSTED,
            )
            self._task_resources[hash(task_resources)] = task_resources
        task.current_task_resources = task_resources
