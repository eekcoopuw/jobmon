"""Workflow Run is an execution instance of a declared workflow."""
import copy
import getpass
import time
from datetime import datetime
from multiprocessing import Process
from typing import Dict, List, Optional, Set, Tuple

from jobmon import __version__
from jobmon.client.client_config import ClientConfig
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.constants import ExecutorParameterSetType, TaskStatus, WorkflowRunStatus
from jobmon.exceptions import InvalidResponse, SchedulerNotAlive, WorkflowNotResumable
from jobmon.requester import Requester, http_request_ok

import structlog as logging

logger = logging.getLogger(__name__)


# This is re-defined into the global namespace of the module so it can be
# safely patched
ValueError = ValueError


class WorkflowRunExecutionStatus(object):
    """Enumerate possible exit statuses for WorkflowRun._execute()"""

    SUCCEEDED = 0
    FAILED = 1
    STOPPED_BY_USER = 2


class WorkflowRun(object):
    """
    WorkflowRun enables tracking for multiple runs of a single Workflow. A
    Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(self, workflow_id: int, executor_class: str,
                 slack_channel: str = 'jobmon-alerts', resume: bool = False,
                 reset_running_jobs: bool = True, resume_timeout: int = 300,
                 requester: Optional[Requester] = None):
        self.workflow_id = workflow_id
        self.executor_class = executor_class
        self.user = getpass.getuser()

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # state tracking
        self.swarm_tasks: Dict[int, SwarmTask] = {}
        self.all_done: Set[SwarmTask] = set()
        self.all_error: Set[SwarmTask] = set()
        self.last_sync = '2010-01-01 00:00:00'

        # bind to database
        app_route = "/client/workflow_run"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={'workflow_id': self.workflow_id,
                     'user': self.user,
                     'executor_class': self.executor_class,
                     'jobmon_version': __version__,
                     'resume': resume,
                     'reset_running_jobs': reset_running_jobs},
            request_type='post',
            logger=logger
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"Invalid Response to {app_route}: {rc}")

        # check if we can continue
        self.workflow_run_id = response['workflow_run_id']
        current_status = response['status']
        previous_wfr = response['previous_wfr']
        if previous_wfr:

            # we can't continue if any of the following are true:
            # 1) there are existing workflow runs and resume is not set
            # 2) current status was returned as error. that indicates a race
            #    condition with another workflow run where they both set the
            #    workflow to created nearly at the same time.
            prev_wfr_id, prev_status = previous_wfr[0]
            if not resume or current_status == WorkflowRunStatus.ERROR:
                raise WorkflowNotResumable(
                    "There are multple active workflow runs already for "
                    f"workflow_id ({self.workflow_id}). Found previous "
                    f"workflow_run_id/status: {prev_wfr_id}/{prev_status}")
            prev_status = self._wait_till_resumable(prev_wfr_id, resume_timeout)

            # workflow wasn't terminated
            hot_resume = prev_status == WorkflowRunStatus.HOT_RESUME and not reset_running_jobs
            if prev_status != WorkflowRunStatus.TERMINATED and not hot_resume:
                app_route = f'/client/workflow_run/{self.workflow_run_id}/delete'
                return_code, response = self.requester.send_request(
                    app_route=app_route,
                    message={},
                    request_type='put',
                    logger=logger
                )
                if http_request_ok(return_code) is False:
                    raise InvalidResponse(
                        f'Unexpected status code {return_code} from PUT '
                        f'request through route {app_route}. Expected '
                        f'code 200. Response content: {response}')
                raise WorkflowNotResumable(
                    "Workflow cannot be created because a previous workflow "
                    "run exists and hasn't terminated. Found previous "
                    f"workflow_run_id/status: {prev_wfr_id}/{prev_status}")

        # workflow was created successfully
        self._status = WorkflowRunStatus.REGISTERED

        # test parameter to force failure
        self._val_fail_after_n_executions = None

    @property
    def status(self) -> str:
        """Status of the workflow run."""
        return self._status

    @property
    def active_tasks(self) -> List[SwarmTask]:
        """List of tasks that are listed as Registered, Done or Error_Fatal."""
        terminal_status = [
            TaskStatus.REGISTERED, TaskStatus.DONE, TaskStatus.ERROR_FATAL]
        return [task for task in self.swarm_tasks.values()
                if task.status not in terminal_status]

    @property
    def scheduler_alive(self) -> bool:
        """If the scheduler process is still active."""
        if not hasattr(self, "_scheduler_proc"):
            return False
        else:
            logger.debug(f"Scheduler proc is: {self._scheduler_proc.is_alive()}")
            return self._scheduler_proc.is_alive()

    @property
    def completed_report(self) -> Tuple:
        """After workflow run has run through, report on success and status."""
        if not hasattr(self, "_completed_report"):
            raise AttributeError("Must executor workflow run before first")
        return self._completed_report

    def update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/update_status'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
        self._status = status

    def execute_interruptible(self, scheduler_proc: Process,
                              fail_fast: bool = False,
                              seconds_until_timeout: int = 36000):
        """Execute the workflow run."""
        # _block_until_any_done_or_error continually checks to make sure this
        # process is alive
        self._scheduler_proc = scheduler_proc

        keep_running = True
        while keep_running:
            try:
                return self._execute(fail_fast, seconds_until_timeout)
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    raise
                else:
                    logger.info("Continuing jobmon execution...")

    def terminate_workflow_run(self) -> None:
        """Terminate the workflow run."""
        app_route = f'/client/workflow_run/{self.workflow_run_id}/terminate'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def _wait_till_resumable(self, wfr_id: int, resume_timeout: int = 300) -> str:
        wait_start = time.time()
        wait_for_resume = True
        while wait_for_resume:
            logger.info(
                f"Waiting for resume. Timeout in {resume_timeout - (time.time() - wait_start)}"
            )
            app_route = f'/client/workflow_run/{wfr_id}/is_resumable'
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={},
                request_type='get',
                logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected '
                    f'code 200. Response content: {response}')

            if response.get("workflow_run_status") is not None:
                wait_for_resume = False
                status = response["workflow_run_status"]
            else:
                if (time.time() - wait_start) > resume_timeout:
                    raise WorkflowNotResumable(
                        "workflow_run timed out waiting for previous "
                        "workflow_run to exit. Try again in a few minutes.")
                else:
                    sleep_time = float(resume_timeout) / 10.
                    time.sleep(sleep_time)

        return status

    def _set_fail_after_n_executions(self, n: int) -> None:
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self._val_fail_after_n_executions = n

    def _get_current_time(self) -> datetime:
        app_route = '/time'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get',
            logger=logger
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
        return response["time"]

    def _execute(self, fail_fast: bool = False,
                 seconds_until_timeout: int = 36000,
                 wedged_workflow_sync_interval: int = 600):
        """
        Take a concrete DAG and queue al the Tasks that are not DONE.

        Uses forward chaining from initial fringe, hence out-of-date is not
        applied transitively backwards through the graph. It could also use
        backward chaining from an identified goal node, the effect is
        identical.

        The internal data structures are lists, but might need to be changed
        to be better at scaling.

        Conceptually:
        Mark all Tasks as not tried for this execution
        while the fringe is not empty:
            if the job is DONE, skip it and add its downstreams to the fringe
            if not, queue it
            wait for some jobs to complete
            rinse and repeat

        :return:
            num_new_completed, num_previously_completed
        """
        self.update_status(WorkflowRunStatus.RUNNING)

        self.last_sync = self._get_current_time()

        # populate sets for all current tasks
        self._parse_adjusting_done_and_errors(list(self.swarm_tasks.values()))
        previously_completed = copy.copy(self.all_done)  # for reporting

        # compute starting fringe
        fringe = self._compute_fringe()

        # test parameter
        logger.debug(
            f"fail_after_n_executions is {self._val_fail_after_n_executions}")
        n_executions = 0

        logger.info(f"Executing Workflow Run {self.workflow_run_id}")

        # These are all Tasks.
        # While there is something ready to be run, or something is running
        while fringe or self.active_tasks:
            # Everything in the fringe should be run or skipped,
            # they either have no upstreams, or all upstreams are marked DONE
            # in this execution

            while fringe:
                # Get the front of the queue and add it to the end.
                # That ensures breadth-first behavior, which is likely to
                # maximize parallelism
                swarm_task = fringe.pop()
                # Start the new jobs ASAP
                if swarm_task.is_done:
                    raise RuntimeError("Invalid DAG. Encountered a DONE node")
                else:
                    logger.debug(f"Instantiating resources for newly ready  task and "
                                 f"changing it to the queued state. Task: {swarm_task},"
                                 f" id: {swarm_task.task_id}")
                    self._adjust_resources_and_queue(swarm_task)

            # TBD timeout?
            # An exception is raised if the runtime exceeds the timeout limit
            completed, failed = self._block_until_any_done_or_error(
                timeout=seconds_until_timeout,
                wedged_workflow_sync_interval=wedged_workflow_sync_interval)
            for swarm_task in completed:
                n_executions += 1
            if failed and fail_fast:
                break  # fail out early
            logger.debug(f"Return from blocking call, completed: "
                         f"{[t.task_id for t in completed]}, "
                         f"failed:{[t.task_id for t in failed]}")

            for swarm_task in completed:
                task_to_add = self._propagate_results(swarm_task)
                fringe = list(set(fringe + task_to_add))
            if (self._val_fail_after_n_executions is not None and
                    n_executions >= self._val_fail_after_n_executions):
                raise ValueError(f"Dag asked to fail after {n_executions} "
                                 f"executions. Failing now")

        # END while fringe or all_active

        # To be a dynamic-DAG tool, we must be prepared for the DAG to have
        # changed. In general we would recompute forward from the fringe.
        # Not efficient, but correct. A more efficient algorithm would be to
        # check the nodes that were added to see if they should be in the
        # fringe, or if they have potentially affected the status of Tasks
        # that were done (error case - disallowed??)

        all_completed = self.all_done
        num_new_completed = len(all_completed) - len(previously_completed)
        all_failed = self.all_error
        if all_failed:
            if fail_fast:
                logger.info("Failing after first failure, as requested")
            logger.info(f"DAG execute ended, failed {all_failed}")
            self.update_status(WorkflowRunStatus.ERROR)
            self._completed_report = (num_new_completed, len(previously_completed))
        else:
            logger.info(f"DAG execute finished successfully, {num_new_completed} jobs")
            self.update_status(WorkflowRunStatus.DONE)
            self._completed_report = (num_new_completed, len(previously_completed))

    def _compute_fringe(self) -> List[SwarmTask]:
        current_fringe: List[SwarmTask] = []
        for swarm_task in self.swarm_tasks.values():
            unfinished_upstreams = []
            for u in swarm_task.upstream_swarm_tasks:
                if u.status != TaskStatus.DONE:
                    unfinished_upstreams.append(u)
                else:
                    # if re-establishing fringe, make sure to re-establish upstream count
                    swarm_task.num_upstreams_done += 1

            # top fringe is defined by:
            # not any unfinished upstream tasks and current task is registered
            is_fringe = (not unfinished_upstreams and
                         swarm_task.status == TaskStatus.REGISTERED)
            if is_fringe:
                current_fringe += [swarm_task]
        return current_fringe

    def _adjust_resources_and_queue(self, swarm_task: SwarmTask) -> None:
        task_id = swarm_task.task_id
        # Create original and validated entries if no params are bound yet
        if not swarm_task.bound_parameters:
            swarm_task.bind_executor_parameters(ExecutorParameterSetType.ORIGINAL)
            swarm_task.bind_executor_parameters(ExecutorParameterSetType.VALIDATED)
        else:
            swarm_task.bind_executor_parameters(ExecutorParameterSetType.ADJUSTED)

        logger.debug(f"Queueing task id: {task_id}")
        swarm_task.queue_task()

    def _block_until_any_done_or_error(self, timeout: int = 36000,
                                       poll_interval: int = 10,
                                       wedged_workflow_sync_interval: int = 600):
        """Block code execution until a task is done or errored"""
        time_since_last_update = 0
        time_since_last_wedge_sync = 0
        while True:
            # make sure we haven't timed out
            if time_since_last_update > timeout:
                raise RuntimeError(f"Not all tasks completed within the given "
                                   f"workflow timeout length ({timeout} "
                                   f"seconds). Submitted tasks will still run,"
                                   f" but the workflow will need to be "
                                   f"restarted.")

            # make sure scheduler is still alive or this is all for nothing
            if not self.scheduler_alive:
                raise SchedulerNotAlive(
                    f"Scheduler process pid=({self._scheduler_proc.pid}) "
                    f"unexpectedly died with exit code "
                    f"{self._scheduler_proc.exitcode}")

            # check if we are doing a full sync or a date based sync
            if time_since_last_wedge_sync > wedged_workflow_sync_interval:
                # should get statuses from every active task that has changed
                # state or any task that has changed state since we last got
                # task status updates
                logger.info("syncing all active tasks")
                swarm_tasks = self._task_status_updates(self.active_tasks)
                time_since_last_wedge_sync = 0
            else:
                # should get statuses of any task that has changed state since
                # we last got task status updates
                swarm_tasks = self._task_status_updates()

            # now parse into sets
            completed, failed, adjusting = (
                self._parse_adjusting_done_and_errors(swarm_tasks))

            # deal with resource errors. we don't want to exit the loop here
            # because this state change doesn't affect the fringe.
            if adjusting:
                for swarm_task in adjusting:
                    # change callable to adjustment function.
                    swarm_task.executor_parameters_callable = \
                        swarm_task.adjust_resources
                    self._adjust_resources_and_queue(swarm_task)

            # exit if fringe is affected
            if completed or failed:
                return completed, failed

            # sleep little baby
            time.sleep(poll_interval)
            time_since_last_update += poll_interval
            time_since_last_wedge_sync += poll_interval

    def _task_status_updates(self, swarm_tasks: List[SwarmTask] = []) -> List[SwarmTask]:
        """Update internal state of tasks to match the database. If no tasks are specified,
        get all.
        """
        swarm_tasks_tuples = [t.to_wire() for t in swarm_tasks]
        app_route = f'/swarm/workflow/{self.workflow_id}/task_status_updates'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'last_sync': str(self.last_sync),
                     'swarm_tasks_tuples': swarm_tasks_tuples},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        self.last_sync = response['time']
        # status gets updated in from_wire
        return [SwarmTask.from_wire(task, self.swarm_tasks) for task in response['task_dcts']]

    def _parse_adjusting_done_and_errors(self, swarm_tasks: List[SwarmTask]) \
            -> Tuple[Set[SwarmTask], Set[SwarmTask], Set[SwarmTask]]:
        """Separate out the done jobs from the errored ones.
        Args:
            tasks (list): list of objects of type models:Task
        """
        completed_tasks = set()
        failed_tasks = set()
        adjusting_tasks = set()
        for swarm_task in swarm_tasks:
            if swarm_task.status == TaskStatus.DONE and swarm_task not in \
                    self.all_done:
                completed_tasks.add(swarm_task)
            elif swarm_task.status == TaskStatus.ERROR_FATAL and swarm_task \
                    not in self.all_error:
                failed_tasks.add(swarm_task)
            elif swarm_task.status == TaskStatus.ADJUSTING_RESOURCES:
                adjusting_tasks.add(swarm_task)
            else:
                continue
        self.all_done.update(completed_tasks)
        self.all_error -= completed_tasks
        self.all_error.update(failed_tasks)
        return completed_tasks, failed_tasks, adjusting_tasks

    def _propagate_results(self, swarm_task: SwarmTask) -> List[SwarmTask]:
        """For all its downstream tasks, is that task now ready to run? Also mark this Task as
        DONE.

        :param task: The task that just completed
        :return: Tasks to be added to the fringe
        """
        new_fringe: List[SwarmTask] = []
        logger.debug(f"Propagate {swarm_task}")
        for downstream in swarm_task.downstream_swarm_tasks:
            logger.debug(f"downstream {downstream}")
            downstream_done = (downstream.status == TaskStatus.DONE)
            downstream.num_upstreams_done += 1
            if (not downstream_done and
                    downstream.status == TaskStatus.REGISTERED):
                if downstream.all_upstreams_done:
                    logger.debug(" and add to fringe")
                    new_fringe += [downstream]  # make sure there's no dups
                else:
                    # don't do anything, task not ready yet
                    logger.debug(" not ready yet")
            else:
                logger.debug(f" not ready yet or already queued, Status is "
                             f"{downstream.status}")
        return new_fringe
