"""Workflow Run is an distributor instance of a declared workflow."""
from datetime import datetime
import logging
import time
from typing import Callable, Dict, Iterator, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.constants import TaskStatus, WorkflowRunStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester


logger = logging.getLogger(__name__)


# This is re-defined into the global namespace of the module so it can be
# safely patched
ValueError = ValueError


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
        workflow_id: int,
        workflow_run_id: int,
        tasks: List[Task],
        fail_after_n_executions: int = 1_000_000_000,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialization of the swarm WorkflowRun."""
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id

        # construct SwarmTasks from Client Tasks
        self.swarm_tasks: Dict[int, SwarmTask] = {}
        for task in tasks:

            task_resources: Optional[TaskResources]
            try:
                task_resources = task.task_resources
            except AttributeError:
                task_resources = None

            # create swarmtasks
            swarm_task = SwarmTask(
                task_id=task.task_id,
                task_hash=hash(task),
                status=task.initial_status,
                cluster=task.cluster,
                task_args_hash=task.task_args_hash,
                task_resources=task_resources,
                resource_scales=task.resource_scales,
                fallback_queues=task.fallback_queues,
                max_attempts=task.max_attempts,
            )
            self.swarm_tasks[task.task_id] = swarm_task

        # create relationships on swarm task
        for task in tasks:
            swarm_task = self.swarm_tasks[task.task_id]
            swarm_task.upstream_swarm_tasks = set(
                [self.swarm_tasks[t.task_id] for t in task.upstream_tasks]
            )
            swarm_task.downstream_swarm_tasks = set(
                [self.swarm_tasks[t.task_id] for t in task.downstream_tasks]
            )

        # state tracking
        self.all_done: Set[SwarmTask] = set()
        self.all_error: Set[SwarmTask] = set()
        self.ready_to_run: List[SwarmTask] = list()

        self.last_sync = datetime.strptime("2010-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        self._status = WorkflowRunStatus.BOUND

        # test parameter to force failure
        self._val_fail_after_n_executions = fail_after_n_executions
        self._n_executions = 0

        # requester
        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    @property
    def status(self) -> str:
        """Status of the workflow run."""
        return self._status

    @property
    def active_tasks(self) -> List[SwarmTask]:
        """List of tasks that are not currently Registered, Adjusting, Done or Error_Fatal."""
        statuses = [
            TaskStatus.REGISTERING,
            TaskStatus.DONE,
            TaskStatus.ERROR_FATAL,
            TaskStatus.ADJUSTING_RESOURCES,
        ]
        return [
            task for task in self.swarm_tasks.values() if task.status not in statuses
        ]

    def update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self.requester.send_request(
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
        self._status = status

    def terminate_workflow_run(self) -> None:
        """Terminate the workflow run."""
        app_route = f"/workflow_run/{self.workflow_run_id}/terminate"
        return_code, response = self.requester.send_request(
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
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        return response["time"]

    def compute_initial_dag_state(self) -> None:
        """Calculate the state of the original DAG."""
        self.last_sync = self._get_current_time()
        self._compute_initial_fringe()
        self._update_dag_state(list(self.swarm_tasks.values()))
        self.num_previously_complete = len(self.all_done)

    def queue_tasks(self) -> Iterator[SwarmTask]:
        """Everything in the to_queue should be run or skipped.

        The tasks either have no upstreams, or all upstreams are marked DONE.
        """
        while self.ready_to_run:
            # Get the front of the queue and add it to the end.
            # That ensures breadth-first behavior, which is likely to
            # maximize parallelism
            swarm_task = self.ready_to_run.pop()
            # Start the new jobs ASAP
            if swarm_task.is_done:
                raise RuntimeError("Invalid DAG. Encountered a DONE node")

            if swarm_task.status == TaskStatus.REGISTERING:
                logger.debug(
                    f"Instantiating resources for newly ready task and "
                    f"changing it to the queued state. Task: {swarm_task},"
                    f" id: {swarm_task.task_id}"
                )
                if swarm_task.task_resources is None:
                    yield swarm_task
                swarm_task.queue_task(self.workflow_run_id)
            elif swarm_task.status == TaskStatus.ADJUSTING_RESOURCES:
                swarm_task.adjust_task_resources()
                swarm_task.queue_task(self.workflow_run_id)
            else:
                raise RuntimeError(
                    f"Task {swarm_task.task_id} in ready_to_run queue but "
                    f"status is {swarm_task.status}."
                )

    def block_until_newly_ready_or_all_done(
        self,
        fail_fast: bool = False,
        poll_interval: int = 10,
        seconds_until_timeout: int = 36000,
        wedged_workflow_sync_interval: int = 600,
        distributor_alive_callable: Optional[Callable] = None,
    ) -> None:
        """Block until there is new work to do, or the workflow run has completed."""
        time_since_last_update = 0
        time_since_last_wedge_sync = 0

        # While something is running and there is nothing ready to run
        while self.active_tasks and not self.ready_to_run:
            # make sure we haven't timed out
            if time_since_last_update > seconds_until_timeout:
                raise RuntimeError(
                    f"Not all tasks completed within the given workflow timeout length "
                    f"({seconds_until_timeout} seconds). Submitted tasks will still run, but "
                    "the workflow will need to be restarted."
                )

            # make sure distributor is still alive or this is all for nothing
            if distributor_alive_callable is not None:
                distributor_alive_callable()

            # check if we are doing a full sync or a date based sync
            if time_since_last_wedge_sync > wedged_workflow_sync_interval:
                # should get statuses from every active task that has changed
                # state or any task that has changed state since we last got
                # task status updates
                logger.info(
                    f"No state changes discovered in {time_since_last_wedge_sync}s. "
                    f"Syncing all tasks to ensure consistency."
                )
                swarm_tasks = self._task_status_updates(self.active_tasks)
                time_since_last_wedge_sync = 0
            else:
                # should get statuses of any task that has changed state since
                # we last got task status updates
                swarm_tasks = self._task_status_updates()

            # now parse into sets
            self._update_dag_state(swarm_tasks)

            # if fail fast and any error
            if fail_fast and self.all_error:
                logger.info("Failing after first failure, as requested")
                raise RuntimeError(
                    "Workflow has failed tasks and fail_fast is set. Failing early."
                )
            # fail during test path
            if self._n_executions >= self._val_fail_after_n_executions:
                raise ValueError(
                    f"WorkflowRun asked to fail after {self._n_executions} "
                    f"executions. Failing now"
                )

            # sleep little baby
            time.sleep(poll_interval)
            time_since_last_update += poll_interval
            time_since_last_wedge_sync += poll_interval

    def _compute_initial_fringe(self) -> None:
        for swarm_task in self.swarm_tasks.values():
            unfinished_upstreams = []
            for u in swarm_task.upstream_swarm_tasks:
                if u.status != TaskStatus.DONE:
                    unfinished_upstreams.append(u)

            # top fringe is defined by:
            # not any unfinished upstream tasks and current task is registered
            if (
                not unfinished_upstreams
                and swarm_task.status == TaskStatus.REGISTERING
                and swarm_task not in self.ready_to_run
            ):
                self.ready_to_run += [swarm_task]

    def _update_dag_state(self, swarm_tasks: List[SwarmTask]) -> None:
        """Given a list of SwarmTasks, update all_done, all_error, and fringe attributes.

        Args:
            swarm_tasks (list): list of swarmtasks
        """
        completed_tasks: Set[SwarmTask] = set()
        failed_tasks: Set[SwarmTask] = set()
        newly_ready: List[SwarmTask] = []
        for swarm_task in swarm_tasks:
            status = swarm_task.status

            if status == TaskStatus.DONE and swarm_task not in self.all_done:
                completed_tasks.add(swarm_task)
                self._n_executions += 1

                # calculate forward the newly ready tasks
                newly_ready.extend(self._new_downstream_fringe(swarm_task))

            elif status == TaskStatus.ERROR_FATAL and swarm_task not in self.all_error:
                failed_tasks.add(swarm_task)

            elif status == TaskStatus.ADJUSTING_RESOURCES:
                newly_ready.append(swarm_task)

            else:
                logger.debug(
                    f"Got status update {status} for task_id: {swarm_task.task_id}."
                    "No actions necessary."
                )
                continue

        # update completed set
        self.all_done.update(completed_tasks)
        if completed_tasks:
            percent_done = round((len(self.all_done) / len(self.swarm_tasks)) * 100, 2)
            logger.info(
                f"{len(completed_tasks)} newly completed tasks. {percent_done} percent done."
            )

        # remove complete tasks from error just in case. update error set
        self.all_error -= completed_tasks
        self.all_error.update(failed_tasks)
        if failed_tasks:
            logger.warning(f"{len(failed_tasks)} newly failed tasks.")

        # add newly ready tasks to the fringe so they can be re-queued
        self.ready_to_run += list(set(newly_ready) - set(self.ready_to_run))

    def _new_downstream_fringe(self, swarm_task: SwarmTask) -> List[SwarmTask]:
        """For all its downstream tasks, is that task now ready to run?

        Args:
            swarm_task: The task that just completed

        Return:
            Tasks to be added to the fringe
        """
        new_fringe: List[SwarmTask] = []
        logger.debug(f"Propagate {swarm_task}")
        for downstream in swarm_task.downstream_swarm_tasks:
            logger.debug(f"downstream {downstream}")
            downstream_done = downstream.status == TaskStatus.DONE
            downstream.num_upstreams_done += 1

            # The adjusting state is not included in this branch because a task can't be in
            # adjusting state until it has already ran. So it can't be part of the new fringe
            # computed from newly done tasks
            if not downstream_done and downstream.status == TaskStatus.REGISTERING:
                if downstream.all_upstreams_done:
                    logger.debug(" and add to fringe")
                    new_fringe += [downstream]  # make sure there's no dups
                else:
                    # don't do anything, task not ready yet
                    logger.debug("Not ready yet")
            else:
                logger.debug(
                    f"Not ready yet or already queued, Status is {downstream.status}"
                )
        return new_fringe

    def _task_status_updates(
        self, swarm_tasks: List[SwarmTask] = None
    ) -> List[SwarmTask]:
        """Update internal state of tasks to match the database.

        If no tasks are specified, get all tasks.
        """
        if swarm_tasks is None:
            swarm_tasks = []
        swarm_tasks_tuples = [t.to_wire() for t in swarm_tasks]
        app_route = f"/workflow/{self.workflow_id}/task_status_updates"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "last_sync": str(self.last_sync),
                "swarm_tasks_tuples": swarm_tasks_tuples,
            },
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
        # status gets updated in from_wire
        return [
            SwarmTask.from_wire(task, self.swarm_tasks)
            for task in response["task_dcts"]
        ]
