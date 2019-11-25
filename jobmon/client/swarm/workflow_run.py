from builtins import str
import copy
from datetime import datetime
from functools import partial
import getpass
from http import HTTPStatus as StatusCodes
import os
import socket
import time

from jobmon.client import shared_requester
from jobmon.client.swarm.job_management.executor_task_instance import \
    ExecutorTaskInstance
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.utils import kill_remote_process
from jobmon.exceptions import CallableReturnedInvalidObject
from jobmon.models.attributes.constants import workflow_run_attribute
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.task_status import TaskStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.client_logging import ClientLogging as logging


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

    def __init__(self, workflow_id, dag_id, stderr, stdout, project,
                 job_instance_state_controller,
                 slack_channel='jobmon-alerts',
                 executor_class='SGEExecutor', working_dir=None,
                 reset_running_jobs=True, requester=shared_requester,
                 bound_tasks={}, fail_fast=False, seconds_until_timeout=36000):
        self.workflow_id = workflow_id
        self.dag_id = dag_id
        self.requester = requester
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.all_done: set = set()
        self.all_error: set = set()
        self.executor_class = executor_class
        self.working_dir = working_dir

        # parameters for execution and state tracking
        self.bound_tasks = bound_tasks  # task hash to BoundTask obj mapping
        self.fail_fast = fail_fast
        self.fail_after_n_executions = None
        self.last_sync = None
        self._sync()
        self.seconds_until_timeout = seconds_until_timeout
        self.job_instance_state_controller = job_instance_state_controller

        self.kill_previous_workflow_runs(reset_running_jobs)
        rc, response = self.requester.send_request(
            app_route='/workflow_run',
            message={'workflow_id': workflow_id,
                     'user': getpass.getuser(),
                     'hostname': socket.gethostname(),
                     'pid': os.getpid(),
                     'stderr': stderr,
                     'stdout': stdout,
                     'project': project,
                     'slack_channel': slack_channel,
                     'executor_class': executor_class,
                     'working_dir': working_dir},
            request_type='post')
        wfr_id = response['workflow_run_id']
        if rc != StatusCodes.OK:
            raise ValueError(f"Invalid Response to add_workflow_run: {rc}")
        self.id = wfr_id

    @property
    def active_tasks(self):
        """List of tasks that are listed as Registered, Done or Error_Fatal"""
        return [task for hash, task in self.bound_tasks.items()
                if task.status not in [TaskStatus.REGISTERED,
                                       TaskStatus.DONE,
                                       TaskStatus.ERROR_FATAL]]

    def check_if_workflow_is_running(self):
        """Query the JQS to see if the workflow is currently running"""
        rc, response = \
            self.requester.send_request(
                app_route=f'/workflow/{self.workflow_id}/workflow_run',
                message={},
                request_type='get')
        if rc != StatusCodes.OK:
            raise ValueError("Invalid Response to is_workflow_running")
        return response['is_running'], response['workflow_run_dct']

    def kill_previous_workflow_runs(self, reset_running_jobs):
        """First check the database for last WorkflowRun... where we store a
        hostname + pid + running_flag

        If in the database as 'running,' check the hostname
        + pid to see if the process is actually still running:

            A) If so, kill those pids and any still running jobs
            B) Then flip the database of the previous WorkflowRun to STOPPED
        """
        status, wf_run = self.check_if_workflow_is_running()
        if not status:
            return
        workflow_run_id = wf_run['id']
        if wf_run['user'] != getpass.getuser():
            msg = ("Workflow_run_id {} for this workflow_id is still in "
                   "running mode by user {}. Please ask this user to kill "
                   "their processes. If they are using the SGE executor, "
                   "please ask them to qdel their jobs. Be aware that if you "
                   "restart this workflow prior to the other user killing "
                   "theirs, this error will not re-raise but you may be "
                   "creating orphaned processes and hard-to-find bugs"
                   .format(workflow_run_id, wf_run['user']))
            logger.error(msg)
            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'workflow_run_id': workflow_run_id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='put')
            raise RuntimeError(msg)
        else:
            kill_remote_process(wf_run['hostname'], wf_run['pid'])
            logger.info(f"Kill previous workflow runs: {workflow_run_id}")
            if reset_running_jobs:
                if wf_run['executor_class'] == "SequentialExecutor":
                    from jobmon.client.swarm.executors.sequential import \
                        SequentialExecutor
                    previous_executor = SequentialExecutor()
                elif wf_run['executor_class'] == "SGEExecutor":
                    from jobmon.client.swarm.executors.sge import SGEExecutor
                    previous_executor = SGEExecutor()
                elif wf_run['executor_class'] == "DummyExecutor":
                    from jobmon.client.swarm.executors.dummy import \
                        DummyExecutor
                    previous_executor = DummyExecutor()
                else:
                    raise ValueError("{} is not supported by this version of "
                                     "jobmon".format(wf_run['executor_class']))
                # get task instances of workflow run
                _, response = self.requester.send_request(
                    app_route=f'/workflow_run/{workflow_run_id}/task_instance',
                    message={},
                    request_type='get')
                task_instances = [ExecutorTaskInstance.from_wire(
                    ti, executor=previous_executor)
                    for ti in response['task_instances']]
                tiid_exid_tuples = [(ti.task_instance_id, ti.executor_id)
                                    for ti in task_instances]
                if task_instances:
                    previous_executor.terminate_task_instances(tiid_exid_tuples)
            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'workflow_run_id': workflow_run_id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='put')

    def update_done(self):
        """Update the status of the workflow_run as done"""
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        """Update the status of the workflow_run as errored"""
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        """Update the status of the workflow_run as stopped"""
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        """Update the status of the workflow_run with whatever status is
        passed
        """
        rc, _ = self.requester.send_request(
            app_route='/workflow_run',
            message={'workflow_run_id': self.id, 'status': status,
                     'status_date': str(datetime.utcnow())},
            request_type='put')

    def add_workflow_run_attribute(self, attribute_type, value):
        """
        Create a workflow_run attribute entry in the database.

        Args:
            attribute_type (int): attribute_type id from
                                  workflow_run_attribute_type table
            value (int): value associated with attribute

        Raises:
            ValueError: If the args are not valid.
                        attribute_type should be int and
                        value should be convertible to int
                        or be string for TAG attribute
        """
        if not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif (not attribute_type == workflow_run_attribute.TAG and not
              int(value)) or (attribute_type == workflow_run_attribute.TAG and
                              not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        else:
            rc, workflow_run_attribute_id = self.requester.send_request(
                app_route='/workflow_run_attribute',
                message={'workflow_run_id': str(self.id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return workflow_run_attribute_id

    def _set_fail_after_n_executions(self, n):
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self.fail_after_n_executions = n

    def execute_interruptible(self):
        keep_running = True
        while keep_running:
            try:
                return self._execute()
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    keep_running = False
                    self.job_instance_state_controller.disconnect()
                    return (WorkflowRunExecutionStatus.STOPPED_BY_USER,
                            len(self.all_done), None, None)
                else:
                    print("Continuing jobmon execution...")
            finally:
                # In a finally block so clean up always occurs
                self._clean_up_after_run()

    def _execute(self):
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
        self.log_workflow_running()
        previously_completed = copy.copy(self.all_done)
        self._set_top_fringe()

        logger.debug("self.fail_after_n_executions is {}"
                     .format(self.fail_after_n_executions))
        fringe = copy.copy(self.top_fringe)
        n_executions = 0

        logger.debug(f"Executing Workflow Run {self.id} with Dag Id "
                     f"{self.dag_id}")

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
                bound_task = fringe.pop()
                # Start the new jobs ASAP
                if bound_task.is_done:
                    raise RuntimeError("Invalid DAG. Encountered a DONE node")
                else:
                    logger.debug(f"Instantiating resources for newly ready "
                                 f"task and changing it to the queued state. "
                                 f"Task: {bound_task}, id: {bound_task.job_id}")
                    self._adjust_resources_and_queue(bound_task)

            # TBD timeout?
            # An exception is raised if the runtime exceeds the timeout limit
            completed_tasks, failed_tasks = self._block_until_any_done_or_error(
                timeout=self.seconds_until_timeout)
            for task in completed_tasks:
                n_executions += 1
            if failed_tasks and self.fail_fast is True:
                break  # fail out early
            logger.debug(f"Return from blocking call, completed: "
                         f"{[t.job_id for t in completed_tasks]}, "
                         f"failed:{[t.job_id for t in failed_tasks]}")

            for task in completed_tasks:
                task_to_add = self._propagate_results(task)
                fringe = list(set(fringe + task_to_add))
            if (self.fail_after_n_executions is not None and
                    n_executions >= self.fail_after_n_executions):
                self.job_instance_state_controller.disconnect()
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
            if self.fail_fast:
                logger.info("Failing after first failure, as requested")
            logger.info(f"DAG execute ended, failed {all_failed}")
            self.job_instance_state_controller.disconnect()
            return (WorkflowRunExecutionStatus.FAILED, num_new_completed,
                    len(previously_completed), len(all_failed))
        else:
            logger.info(f"DAG execute finished successfully, "
                        f"{num_new_completed} jobs")
            self.job_instance_state_controller.disconnect()
            return (WorkflowRunExecutionStatus.SUCCEEDED, num_new_completed,
                    len(previously_completed), len(all_failed))

    def _set_top_fringe(self):
        self.top_fringe = []
        for task in self.bound_tasks.values():
            unfinished_upstreams = [u for u in task.upstream_tasks
                                    if u.status != TaskStatus.DONE]
            if not unfinished_upstreams and \
                    task.status == TaskStatus.REGISTERED:
                self.top_fringe += [task]
        return self.top_fringe

    def log_workflow_running(self) -> None:
        rc, _ = self.requester.send_request(
            app_route=f'/workflow/{self.workflow_id}/log_running',
            message={},
            request_type='post')
        return rc

    def _clean_up_after_run(self) -> None:
        """
            Make sure all the threads are stopped. The JLM is created in
            bind_db
        """
        self.job_instance_state_controller.disconnect()

    def _adjust_resources_and_queue(self, bound_task: SwarmTask):
        task_id = bound_task.task_id
        # Create original and validated entries if no params are bound yet
        if not bound_task.bound_parameters:
            self._bind_parameters(task_id, ExecutorParameterSetType.ORIGINAL,
                                  bound_task=bound_task)
            self._bind_parameters(task_id, ExecutorParameterSetType.VALIDATED,
                                  bound_task=bound_task)
        else:
            self._bind_parameters(task_id, ExecutorParameterSetType.ADJUSTED,
                                  bound_task=bound_task)
        logger.debug(f"Queueing task id: {task_id}")
        bound_task.queue_task()

    def _bind_parameters(self, task_id: int,
                         executor_parameter_set_type: ExecutorParameterSetType,
                         **kwargs):
        bound_task = kwargs.get("bound_task")
        resources = bound_task.executor_parameters(kwargs)
        if not isinstance(resources, ExecutorParameters):
            raise CallableReturnedInvalidObject(f"The function called to "
                                                f"return resources did not "
                                                f"return the expected Executor"
                                                f" Parameters object, it is of"
                                                f" type {type(resources)}")
        bound_task.bound_parameters.append(resources)

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
            tasks = self._get_task_statuses()
            completed, failed, adjusting = self._parse_adjusting_done_and_errors(tasks)
            if adjusting:
                for task in adjusting:
                    # change callable to adjustment function
                    task.executor_parameters = partial(self.adjust_resources, task)
                    self._adjust_resources_and_queue(task)
            if completed or failed:
                return completed, failed
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def _sync(self):
        """Get all tasks from the database and parse the done and errored"""
        tasks = self._get_task_statuses()
        self._parse_adjusting_done_and_errors(tasks)

    def _get_task_statuses(self):
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

        tasks = [SwarmTask.from_wire(task) for task in response['task_dcts']]
        for task in tasks:
            if task.task_args_hash in self.bound_tasks.keys():
                self.bound_tasks[task.task_args_hash].status = task.status
            else:
                # This should really only happen the first time
                # _sync() is called when resuming a WF/DAG. This
                # BoundTask really only serves as a
                # dummy/placeholder for status until the Task can
                # actually be bound. To put it another way, if not
                # this_bound_task.is_bound: ONLY USE IT TO DETERMINE
                # IF THE TASK WAS PREVIOUSLY DONE. This branch may
                # be subject to removal altogether if it can be
                # determined that there is a better way to determine
                # previous state in resume cases
                self.bound_tasks[task.task_args_hash] = \
                    SwarmTask(task=task.task_id, status=task.status,
                              task_args_hash=task.task_args_hash,
                              placeholder=True)
        return tasks

    def _parse_adjusting_done_and_errors(self, tasks):
        """Separate out the done jobs from the errored ones
        Args:
            tasks (list): list of objects of type models:Task
        """
        completed_tasks = set()
        failed_tasks = set()
        adjusting_tasks = set()
        for task in tasks:
            bound_task = self.bound_tasks[task.job_hash]
            if bound_task.status == TaskStatus.DONE and bound_task not in \
                    self.all_done:
                completed_tasks.add(bound_task)
            elif (bound_task.status == TaskStatus.ERROR_FATAL and bound_task
                  not in self.all_error and bound_task.is_bound):
                # if the task is not yet bound, then we must be resuming the
                # workflow. In that case, we do not want to account for this
                # task in our current list of failures as it will be reset
                # and retried... i.e. move on to the else: continue
                failed_tasks.add(bound_task)
            elif bound_task.status == TaskStatus.ADJUSTING_RESOURCES and \
                    bound_task.is_bound:
                adjusting_tasks.add(bound_task)
            else:
                continue
        self.all_done.update(completed_tasks)
        self.all_error -= completed_tasks
        self.all_error.update(failed_tasks)
        return completed_tasks, failed_tasks, adjusting_tasks

    def adjust_resources(self, task, *args, **kwargs):
        """Function from Job Instance Factory that adjusts resources and then
           queues them, this should also incorporate resource binding if they
           have not yet been bound"""
        logger.debug("Job in A state, adjusting resources before queueing")

        # get the most recent parameter set
        exec_param_set = task.bound_parameters[-1]
        only_scale = list(exec_param_set.resource_scales.keys())
        rc, msg = self.requester.send_request(
            app_route=f'/task/{task.task_id}/most_recent_ti_error',
            message={},
            request_type='get')
        if 'exceed_max_runtime' in msg and 'max_runtime_seconds' in only_scale:
            only_scale = ['max_runtime_seconds']
        logger.debug(f"Only going to scale the following resources: "
                     f"{only_scale}")
        resources_adjusted = {'only_scale': only_scale}
        exec_param_set.adjust(**resources_adjusted)
        return exec_param_set

    def _propagate_results(self, task: SwarmTask):
        """
        For all its downstream tasks, is that task now ready to run?
        Also mark this Task as DONE

        :param task: The task that just completed
        :return: Tasks to be added to the fringe
        """
        new_fringe = []
        logger.debug(f"Propagate {task}")
        for downstream in task.downstream_tasks:
            logger.debug(f"downstream {downstream}")
            downstream_done = (downstream.status == TaskStatus.DONE)
            if (not downstream_done and
                    downstream.status == TaskStatus.REGISTERED):
                if downstream.all_upstreams_done:
                    logger.debug(" and add to fringe")
                    new_fringe += [downstream] # make sure there's no dups
                else:
                    # don't do anything, task not ready yet
                    logger.debug(" not ready yet")
            else:
                logger.debug(f" not ready yet or already queued, Status is "
                             f"{downstream.status}")
        return new_fringe



