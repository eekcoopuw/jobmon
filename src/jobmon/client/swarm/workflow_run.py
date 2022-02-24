"""Workflow Run is an distributor instance of a declared workflow."""
from __future__ import annotations

from datetime import datetime
import logging
import time
from typing import Callable, Dict, Iterator, List, Optional, Set, TYPE_CHECKING

from jobmon.client.client_config import ClientConfig
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.task_resources import TaskResources
from jobmon.constants import TaskStatus, WorkflowRunStatus, TaskResourcesType
from jobmon.exceptions import InvalidResponse, CallableReturnedInvalidObject

from jobmon.requester import http_request_ok, Requester

# avoid circular imports on backrefs
if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow


logger = logging.getLogger(__name__)


# This is re-defined into the global namespace of the module so it can be
# safely patched
ValueError = ValueError


class SwarmCommand:

    def __init__(
        self,
        func: Callable[..., Optional[List[SwarmCommand]]],
        *args,
        **kwargs
    ):
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state
            *args: positional args to be passed into func
            **kwargs: kwargs to be to be passed into func
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.error_raised = False

    def __call__(self):
        try:
            self._func(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e
            self.error_raised = True


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
        fail_after_n_executions: int = 1_000_000_000,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialization of the swarm WorkflowRun."""
        self.workflow_run_id = workflow_run_id

        # state tracking
        self.tasks: Dict[int, SwarmTask] = {}
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
        # self._swarm_commands: List[SwarmCommand] = []

        # flow control
        self.fail_fast = fail_fast

        # test parameters to force failure
        self._val_fail_after_n_executions = fail_after_n_executions
        self._n_executions = 0

        # optional config
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        if requester is None:
            requester = Requester(ClientConfig.from_defaults().url)
        self._requester = requester

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

    def from_workflow(self, workflow: Workflow) -> None:
        self.workflow_id = workflow.workflow_id

        # construct SwarmTasks from Client Tasks
        for task in workflow.tasks.values():

            if task.fallback_queues is not None:
                cluster = workflow.get_cluster_by_name(task.cluster_name)
                fallback_queues = []
                for queue in task.fallback_queues:
                    cluster_queue = cluster.get_queue(queue)
                    fallback_queues.append(cluster_queue)

            # create swarmtasks
            swarm_task = SwarmTask(
                task_id=task.task_id,
                status=task.initial_status,
                max_attempts=task.max_attempts,
                cluster=cluster,
                task_resources=task.original_task_resources,
                compute_resources_callable=task.compute_resources_callable,
                resource_scales=task.resource_scales,
                fallback_queues=fallback_queues,
            )
            self.tasks[task.task_id] = swarm_task

        for task in workflow.tasks.values():
            # create relationships on swarm task
            swarm_task = self.tasks[task.task_id]
            swarm_task.upstream_swarm_tasks = set(
                [self.tasks[t.task_id] for t in task.upstream_tasks]
            )
            swarm_task.downstream_swarm_tasks = set(
                [self.tasks[t.task_id] for t in task.downstream_tasks]
            )

            # assign each task to the correct set
            self._task_status_map[swarm_task.status].add(swarm_task)

            # compute initial fringe
            if swarm_task.status == TaskStatus.DONE:
                # if task is done check if there are downstreams that can run
                for downstream in swarm_task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1

        self.last_sync = self._get_current_time()
        self.num_previously_complete = len(self._task_status_map[TaskStatus.DONE])

    def run(self, distributor_alive_callable: Callable[..., bool]):
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
            logger.info(f"Executing Workflow Run {self.workflow_run_id}")
            self._update_status(WorkflowRunStatus.RUNNING)

            while self.status == WorkflowRunStatus.RUNNING:
                try:

                    loop_start = time.time()

                    # process any commands that we can
                    self.process_commands()

                    # take a break if needed
                    if not self._needs_synchronization:
                        loop_duration = time.time() - loop_start
                        time.sleep(self._workflow_run_heartbeat_interval - loop_duration)

                    # then synchronize state
                    self.synchronize_state()

                # user interrupt
                except KeyboardInterrupt:
                    logger.warning("Keyboard interrupt raised")
                    confirm = input("Are you sure you want to exit (y/n): ")
                    confirm = confirm.lower().strip()
                    if confirm == "y":
                        self._update_status(WorkflowRunStatus.STOPPED)
                    else:
                        logger.info("Continuing jobmon...")

        except Exception:
            self._update_status(WorkflowRunStatus.ERROR)
            raise

        finally:
            terminating_states = [
                WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME,
                WorkflowRunStatus.STOPPED
            ]
            if self.status in terminating_states:
                logger.warning(
                    f"Workflow Run set to {self.status}. Attempting graceful shutdown."
                )
                self._terminate_task_instances()
                self._block_until_all_stopped()
                self._update_status(WorkflowRunStatus.TERMINATED)

    @property
    def _needs_synchronization(self) -> bool:
        time_diff = time.time() - self._last_heartbeat_time
        return time_diff > self._workflow_run_heartbeat_interval

    def get_swarm_commands(self) -> Iterator[SwarmCommand]:
        """Get and iterator of work to be done."""
        for task in self._task_status_map[TaskStatus.ADJUSTING_RESOURCES]:
            yield SwarmCommand(self.adjust_task, task)
        for task in self._task_status_map[TaskStatus.REGISTERING]:
            if task.all_upstreams_done:
                yield SwarmCommand(self.queue_task, task)

    def process_commands(self):
        """Processes swarm commands until all work is done or we need to synchronize state."""
        keep_processing = not self._needs_synchronization
        swarm_commands = self.get_swarm_commands()

        while keep_processing:

            # run commands
            try:

                # use an iterator so we don't waste compute
                swarm_command = next(swarm_commands)
                swarm_command()
                if swarm_command.error_raised:
                    logger.error(swarm_command.exception)

                # keep processing commands if we don't need a status sync
                keep_processing = not self._needs_synchronization

            except StopIteration:
                # stop processing commands if we are out of commands
                keep_processing = False

    def synchronize_state(self, full_sync: bool = False) -> None:
        self._log_heartbeat()

        # TODO: should we be excluding DONE and ERROR_FATAL?
        if full_sync:
            updated_tasks = self._sync_task_statuses(set(self.tasks.values()))

        else:
            updated_tasks = self._sync_task_statuses()

        # remove these tasks from old mapping
        for status in self._task_status_map.keys():
            self._task_status_map[status] = self._task_status_map[status] - updated_tasks

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

            elif status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            else:
                logger.debug(
                    f"Got status update {status} for task_id: {task.task_id}."
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

            # check if done
            if len(self.tasks) == len(self._task_status_map[TaskStatus.DONE]):
                logger.info("All tasks are done")
                self._update_status(WorkflowRunStatus.DONE)

        # if newly failed, report failures and check if we should error out
        if num_newly_failed > 0:
            logger.warning(f"{num_newly_failed} newly failed tasks.")

            # if fail fast and any error
            if self.fail_fast and self._task_status_map[TaskStatus.ERROR_FATAL]:
                logger.info("Failing after first failure, as requested")
                self._update_status(WorkflowRunStatus.ERROR)
            # fail during test path
            if self._n_executions >= self._val_fail_after_n_executions:
                logger.info(f"WorkflowRun asked to fail after {self._n_executions} "
                            "executions. Failing now")
                self._update_status(WorkflowRunStatus.ERROR)

    def _log_heartbeat(self):
        next_report_increment = (
            self._workflow_run_heartbeat_interval * self._heartbeat_report_by_buffer
        )
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={"status": self._status,
                     "next_report_increment": next_report_increment},
            request_type="put",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]

    def _block_until_all_stopped(self) -> None:
        pass

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        self._status = status

        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

    def _terminate_task_instances(self) -> None:
        """Terminate the workflow run."""
        app_route = f"/workflow_run/{self.workflow_run_id}/terminate_task_instances"
        return_code, response = self._requester.send_request(
            app_route=app_route, message={}, request_type="put", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

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
            app_route=app_route, message={}, request_type="get", logger=logger
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        return response["time"]

    def _refresh_status_map(self, tasks: Set[SwarmTask]) -> None:
        # remove these tasks from old mapping
        for status in self._task_status_map.keys():
            self._task_status_map[status] = self._task_status_map[status] - tasks

        num_newly_completed = 0
        num_newly_failed = 0
        for task in tasks:

            # assign each task to the correct set
            self._task_status_map[task.status].add(task)

            if task.status == TaskStatus.DONE:
                num_newly_completed += 1
                self._n_executions += 1  # a test param

                # if task is done check if there are downstreams that can run
                for downstream in task.downstream_swarm_tasks:
                    downstream.num_upstreams_done += 1

            elif status == TaskStatus.ERROR_FATAL:
                num_newly_failed += 1

            else:
                logger.debug(
                    f"Got status update {status} for task_id: {task.task_id}."
                    "No actions necessary."
                )

        if num_newly_failed > 0:
            percent_done = round(
                (len(self._task_status_map[TaskStatus.DONE]) / len(self.tasks)) * 100, 2
            )
            logger.info(
                f"{num_newly_completed} newly completed tasks. {percent_done} percent done."
            )

        if num_newly_failed > 0:
            logger.warning(f"{num_newly_failed} newly failed tasks.")

    def _sync_task_statuses(self, tasks: Set[SwarmTask] = None) -> Set[SwarmTask]:
        """Update internal state of tasks to match the database.

        If no tasks are specified, get all tasks.
        """
        if tasks is None:
            tasks = set()
        task_tuples = [(t.task_id, t.status) for t in tasks]
        app_route = f"/workflow/{self.workflow_id}/task_status_updates"
        return_code, response = self._requester.send_request(
            app_route=app_route,
            message={"last_sync": str(self.last_sync),
                     "swarm_tasks_tuples": task_tuples},
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        self.last_sync = response["time"]

        new_status_tasks: Set[SwarmTask] = set()
        for wire_tuple in response["task_dcts"]:
            task_id = int(wire_tuple[0])
            new_status = wire_tuple[1]

            # mutate the task
            task = self.tasks[task_id]
            if new_status != task.status:
                task.status = new_status
                new_status_tasks.add(task)

        return new_status_tasks

    def queue_task(self, task: SwarmTask) -> None:
        self._task_status_map[task.status].remove(task)
        self._set_validated_task_resources(task)
        task.queue_task(self.workflow_run_id)
        self._task_status_map[task.status].add(task)

    def adjust_task(self, task: SwarmTask) -> None:
        self._task_status_map[task.status].remove(task)
        self._set_adjusted_task_resources(task)
        task.queue_task(self.workflow_run_id)
        self._task_status_map[task.status].add(task)

    def _set_validated_task_resources(self, task: SwarmTask) -> None:

        # get cluster and original params
        cluster = task.cluster
        resource_params = task.task_resources.concrete_resources.resources.copy()
        queue = task.task_resources.concrete_resources.queue

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

        # construct task_resources
        try:
            time_object = datetime.strptime(resource_params["runtime"], "%H:%M:%S")
            time_seconds = (
                time_object.hour * 60 * 60
                + time_object.minute * 60
                + time_object.second
            )
            resource_params["runtime"] = str(time_seconds) + "s"
        except Exception:
            pass

        _, _, concrete_resource = \
            cluster.concrete_resource_class.validate_and_create_concrete_resource(
                queue, resource_params
            )

        # if validated concrete resources are different than original. get new resource object
        validated_resource_hash = hash(concrete_resource)
        if validated_resource_hash != hash(task.task_resources.concrete_resources):
            try:
                task_resources = self._task_resources[hash(concrete_resource)]
            except KeyError:
                task_resources = TaskResources(
                    concrete_resources=concrete_resource,
                    task_resources_type_id=TaskResourcesType.VALIDATED
                )
                task_resources.bind()
                self._task_resources[hash(task_resources)] = task_resources

        task.task_resources = task_resources

    def _set_adjusted_task_resources(self, task: SwarmTask) -> None:
        """Adjust the swarm task's parameters.

        Use the cluster API to generate the new resources, then bind to input swarmtask.
        """
        # current resources
        resource_params = task.task_resources.concrete_resources.resources.copy()

        concrete_resource = \
            task.cluster.concrete_resource_class.adjust_and_create_concrete_resource(
                existing_resources=resource_params,
                resource_scales=task.resource_scales,
                expected_queue=task.task_resources.queue,
                fallback_queues=task.fallback_queues,
            )

        try:
            task_resources = self._task_resources[hash(concrete_resource)]
        except KeyError:
            task_resources = TaskResources(
                concrete_resources=concrete_resource,
                task_resources_type_id=TaskResourcesType.ADJUSTED,
            )
            task_resources.bind()
            self._task_resources[hash(task_resources)] = task_resources

        task.task_resources = task_resources
