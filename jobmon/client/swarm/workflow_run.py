from builtins import str
import copy
from functools import partial
import getpass
from http import HTTPStatus as StatusCodes
import time
from typing import Dict, Set, List, Tuple

from jobmon import __version__
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm import shared_requester
from jobmon.client.swarm import SwarmLogging as logging
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.exceptions import (CallableReturnedInvalidObject, InvalidResponse,
                               MultipleWorkflowRuns)
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.task_status import TaskStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus

logger = logging.getLogger(__name__)


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
                 reset_running_jobs: bool = True,
                 requester: Requester = shared_requester):
        self.workflow_id = workflow_id
        self.executor_class = executor_class
        self.user = getpass.getuser()

        self.requester = requester

        # state tracking
        self.swarm_tasks: Dict[int, SwarmTask] = {}
        self.all_done: Set[SwarmTask] = set()
        self.all_error: Set[SwarmTask] = set()
        self.last_sync = None

        # bind to database
        # TODO: figure out whether we need slack channel, node, and pid in db
        app_route = "workflow_run"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={'workflow_id': self.workflow_id,
                     'user': self.user,
                     'executor_class': self.executor_class,
                     'jobmon_version': __version__,
                     'resume': resume},
            request_type='post')
        if rc != StatusCodes.OK:
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
            prev_wfr_id, prev_status = previous_wfr
            if not resume or current_status == WorkflowRunStatus.ERROR:
                raise MultipleWorkflowRuns(
                    "There are multple active workflow runs already for "
                    f"workflow id ({self.workflow_id}). Found previous "
                    f"workflow_run_id/status: {prev_wfr_id}/{prev_status}")

            # TODO: consider a force flag if previous workflow never
            # signals that it's dead. in that case this would be in a
            # try/catch block and we'd only raise an error if force was
            # false (the default). Alternatively, perhaps this should be based
            # on whether we are recieving heartbeats from the old workflow
            self._wait_for_resume(prev_wfr_id, prev_status)

        # test parameter to force failure
        self.fail_after_n_executions = None

    @property
    def active_tasks(self):
        """List of tasks that are listed as Registered, Done or Error_Fatal"""
        terminal_status = [
            TaskStatus.REGISTERED, TaskStatus.DONE, TaskStatus.ERROR_FATAL]
        return [task for task in self.swarm_tasks.values()
                if task.status not in terminal_status]

    def execute_interruptible(self, fail_fast: bool = False):
        keep_running = True
        while keep_running:
            try:
                return self._execute(fail_fast=fail_fast)
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    keep_running = False
                    return (WorkflowRunExecutionStatus.STOPPED_BY_USER,
                            len(self.all_done), None, None)
                else:
                    print("Continuing jobmon execution...")

    def update_status(self, status):
        """Update the status of the workflow_run with whatever status is
        passed
        """
        app_route = f'/workflow_run/{self.workflow_run_id}'
        rc, _ = self.requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='put')

    def _wait_for_resume(self, wfr_id: int, status: str):
        pass

    def _set_fail_after_n_executions(self, n):
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self.fail_after_n_executions = n

    def _execute(self, fail_fast: bool = False,
                 seconds_until_timeout: int = 36000):
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
            A triple: True, len(all_completed_tasks), len(all_failed_tasks)
        """
        self.update_status(WorkflowRunStatus.RUNNING)

        # If none are in running or created then we need to reset failed
        # jobs and sync.
        self._parse_adjusting_done_and_errors(list(self.swarm_tasks.values()))

        previously_completed = copy.copy(self.all_done)
        self._compute_fringe()

        logger.debug(
            f"self.fail_after_n_executions is {self.fail_after_n_executions}")
        fringe = self._compute_fringe()
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
                    logger.debug(
                        f"Instantiating resources for newly ready  task and "
                        f"changing it to the queued state. Task: {swarm_task},"
                        f" id: {swarm_task.task_id}")
                    self._adjust_resources_and_queue(swarm_task)

            # TBD timeout?
            # An exception is raised if the runtime exceeds the timeout limit
            completed, failed = self._block_until_any_done_or_error(
                timeout=seconds_until_timeout)
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
            if (self.fail_after_n_executions is not None and
                    n_executions >= self.fail_after_n_executions):
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
            return (WorkflowRunExecutionStatus.FAILED, num_new_completed,
                    len(previously_completed), len(all_failed))
        else:
            logger.info(f"DAG execute finished successfully, "
                        f"{num_new_completed} jobs")
            return (WorkflowRunExecutionStatus.SUCCEEDED, num_new_completed,
                    len(previously_completed), len(all_failed))

    def _compute_fringe(self) -> List[SwarmTask]:
        current_fringe: List[SwarmTask] = []
        for swarm_task in self.swarm_tasks.values():
            unfinished_upstreams = [u for u in swarm_task.upstream_swarm_tasks
                                    if u.status != TaskStatus.DONE]

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
            self._bind_parameters(task_id, ExecutorParameterSetType.ORIGINAL,
                                  swarm_task=swarm_task)
            self._bind_parameters(task_id, ExecutorParameterSetType.VALIDATED,
                                  swarm_task=swarm_task)
        else:
            self._bind_parameters(task_id, ExecutorParameterSetType.ADJUSTED,
                                  swarm_task=swarm_task)
        logger.debug(f"Queueing task id: {task_id}")
        swarm_task.queue_task()

    def _bind_parameters(self, task_id: int,
                         executor_parameter_set_type: ExecutorParameterSetType,
                         **kwargs):
        swarm_task = kwargs.get("swarm_task")
        resources = swarm_task.executor_parameters(kwargs)
        if not isinstance(resources, ExecutorParameters):
            raise CallableReturnedInvalidObject(
                "The function called to return resources did not return the "
                "expected Executor Parameters object, it is of type "
                f"{type(resources)}")
        swarm_task.bound_parameters.append(resources)

        if executor_parameter_set_type == ExecutorParameterSetType.VALIDATED:
            resources.validate()
        self._add_parameters(task_id, resources, executor_parameter_set_type)

    def _add_parameters(self, task_id: int,
                        executor_parameters: ExecutorParameters,
                        parameter_set_type: ExecutorParameterSetType =
                        ExecutorParameterSetType.VALIDATED):
        """Add an entry for the validated parameters to the database and
           activate them"""
        msg = {'parameter_set_type': parameter_set_type}
        msg.update(executor_parameters.to_wire())

        self.requester.send_request(
            app_route=f'/job/{task_id}/update_resources',
            message=msg,
            request_type='post')

    def _block_until_any_done_or_error(self, timeout=36000, poll_interval=10):
        """Block code execution until a task is done or errored"""
        time_since_last_update = 0
        while True:
            if time_since_last_update > timeout:
                raise RuntimeError(f"Not all tasks completed within the given "
                                   f"workflow timeout length ({timeout} "
                                   f"seconds). Submitted tasks will still run,"
                                   f" but the workflow will need to be "
                                   f"restarted.")
            swarm_tasks = self._sync_task_statuses()
            completed, failed, adjusting = (
                self._parse_adjusting_done_and_errors(swarm_tasks))
            if adjusting:
                for swarm_tasks in adjusting:
                    # change callable to adjustment function
                    swarm_tasks.executor_parameters = partial(
                        self.adjust_resources, swarm_tasks)
                    self._adjust_resources_and_queue(swarm_tasks)
            if completed or failed:
                return completed, failed
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def _sync_task_statuses(self) -> List[SwarmTask]:
        """Query the database for the status of all jobs"""
        if self.last_sync:
            rc, response = self.requester.send_request(
                app_route=f'/workflow/{self.workflow_id}/task_status',
                message={'last_sync': str(self.last_sync)},
                request_type='get'
            )
        else:
            rc, response = self.requester.send_request(
                app_route=f'/workflow/{self.workflow_id}/task_status',
                message={},
                request_type='get')
        logger.debug(f"Workflow run: get_task_statuses(): rc is {rc} and "
                     f"response is {response}")
        utcnow = response['time']
        self.last_sync = utcnow

        # status gets updated in from_wire
        swarm_tasks: List[SwarmTask] = [
            SwarmTask.from_wire(task, self.swarm_tasks)
            for task in response['task_dcts']]
        return swarm_tasks

    def _parse_adjusting_done_and_errors(self, swarm_tasks: List[SwarmTask]) \
            -> Tuple[Set[SwarmTask], Set[SwarmTask], Set[SwarmTask]]:
        """Separate out the done jobs from the errored ones
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

    def adjust_resources(self, swarm_task: SwarmTask, *args, **kwargs):
        """Function from Job Instance Factory that adjusts resources and then
           queues them, this should also incorporate resource binding if they
           have not yet been bound"""
        logger.debug("Job in A state, adjusting resources before queueing")

        # get the most recent parameter set
        exec_param_set = swarm_task.bound_parameters[-1]
        only_scale = list(exec_param_set.resource_scales.keys())
        rc, msg = self.requester.send_request(
            app_route=f'/task/{swarm_task.task_id}/most_recent_ti_error',
            message={},
            request_type='get')
        if 'exceed_max_runtime' in msg and 'max_runtime_seconds' in only_scale:
            only_scale = ['max_runtime_seconds']
        logger.debug(f"Only going to scale the following resources: "
                     f"{only_scale}")
        resources_adjusted = {'only_scale': only_scale}
        exec_param_set.adjust(**resources_adjusted)
        return exec_param_set

    def _propagate_results(self, swarm_task: SwarmTask) -> List[SwarmTask]:
        """
        For all its downstream tasks, is that task now ready to run?
        Also mark this Task as DONE

        :param task: The task that just completed
        :return: Tasks to be added to the fringe
        """
        new_fringe = []
        logger.debug(f"Propagate {swarm_task}")
        for downstream in swarm_task.downstream_swarm_tasks:
            logger.debug(f"downstream {downstream}")
            downstream_done = (downstream.status == TaskStatus.DONE)
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
