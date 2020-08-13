from __future__ import annotations
from http import HTTPStatus as StatusCodes
import multiprocessing as mp
import sys
import threading
import time
from typing import Optional, List

import tblib.pickling_support

from jobmon.client import ClientLogging as logging
from jobmon.client import shared_requester
from jobmon.client.execution.strategies.base import Executor
from jobmon.client.execution.scheduler.execution_config import ExecutionConfig
from jobmon.client.execution.scheduler.executor_task import ExecutorTask
from jobmon.client.execution.scheduler.executor_task_instance import \
    ExecutorTaskInstance
from jobmon.client.requests.requester import Requester, http_request_ok
from jobmon.exceptions import InvalidResponse, WorkflowRunStateError, ResumeSet
from jobmon.models.constants import qsub_attribute
from jobmon.models.workflow_run_status import WorkflowRunStatus


logger = logging.getLogger(__name__)
tblib.pickling_support.install()


class ExceptionWrapper(object):

    def __init__(self, ee):
        self.ee = ee
        __,  __, self.tb = sys.exc_info()

    def re_raise(self):
        raise self.ee.with_traceback(self.tb)


class TaskInstanceScheduler:

    def __init__(self, workflow_id: int, workflow_run_id: int,
                 executor: Executor,
                 config: ExecutionConfig = ExecutionConfig.from_defaults(),
                 requester: Optional[Requester] = None):

        #
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.executor = executor
        self.config = config
        if requester is None:
            requester = shared_requester
        self.requester = requester
        logger.info(f"scheduler: communicating at {self.requester.url}")

        # work to do
        self._to_instantiate: List[ExecutorTask] = []
        self._to_reconcile: List[ExecutorTaskInstance] = []

        # log heartbeat on startup so workflow run FSM doesn't have any races
        self.heartbeat()

    def run_scheduler(self, stop_event: Optional[mp.synchronize.Event] = None,
                      status_queue: Optional[mp.Queue] = None):
        try:
            # start up the worker thread and executor
            if not self.executor.started:
                self.executor.start(self.config.jobmon_command)
            logger.info(f"scheduler: executor has started")

            # send response back to main
            if status_queue is not None:
                status_queue.put("ALIVE")

            # work loop is always running in a separate thread
            thread_stop_event = threading.Event()
            thread = threading.Thread(
                target=self._schedule_forever,
                args=(thread_stop_event, self.config.scheduler_poll_interval))
            thread.daemon = True
            thread.start()

            # infinite blocking loop unless resume or stop requested from
            # main process
            self._heartbeats_forever(
                self.config.workflow_run_heartbeat_interval, stop_event)

            # stop worker thread
            thread_stop_event.set()

        except ResumeSet as e:
            # stop doing new work otherwise terminate won't work properly
            thread_stop_event.set()
            max_loops = 10
            loops = 0

            # this shouldn't take more than a few seconds. 20s is plenty
            while thread.is_alive() and loops < max_loops:
                time.sleep(2)
                loops += 1

            # terminate jobs via executor API
            self._terminate_active_task_instances()

            # send error back to main
            if status_queue is not None:
                status_queue.put(ExceptionWrapper(e))
            else:
                raise

        except Exception as e:
            # send error back to main
            if status_queue is not None:
                status_queue.put(ExceptionWrapper(e))
            else:
                raise

        finally:
            # stop executor
            self.executor.stop()

            if status_queue is not None:
                status_queue.put("SHUTDOWN")

    def heartbeat(self) -> None:
        # log heartbeats for tasks queued for batch execution and for the
        # workflow run
        logger.info(f"scheduler: logging heartbeat")
        self._log_executor_report_by()
        self._log_workflow_run_heartbeat()

    def schedule(self, thread_stop_event: Optional[threading.Event] = None
                 ) -> None:
        logger.info(f"scheduler: scheduling work. reconciling errors.")
        # get work if there isn't any in the queues
        if not self._to_instantiate and not self._to_reconcile:
            self._get_tasks_queued_for_instantiation()
            logger.debug(f"Found {len(self._to_instantiate)} Queued Tasks")
            self._get_lost_task_instances()
            logger.debug(f"Found {len(self._to_reconcile)} Lost Tasks")

        # iterate through all work to do unless a stop event is set from the
        # main thread
        while self._keep_scheduling(thread_stop_event):
            if self._to_instantiate:
                task = self._to_instantiate.pop(0)
                self._create_task_instance(task)
            if self._to_reconcile:
                task_instance = self._to_reconcile.pop(0)
                task_instance.log_error()

    def _heartbeats_forever(
            self, heartbeat_interval: int = 90,
            process_stop_event: Optional[mp.synchronize.Event] = None) -> None:
        keep_beating = True
        while keep_beating:
            self.heartbeat()

            # check if we need to interrupt
            if process_stop_event is not None:
                if process_stop_event.wait(timeout=heartbeat_interval):
                    keep_beating = False
            else:
                time.sleep(heartbeat_interval)

    def _keep_scheduling(
            self, thread_stop_event: Optional[threading.Event] = None) -> bool:
        any_work_to_do = any(self._to_instantiate) or any(self._to_reconcile)
        # If we are running in a thread. This is the standard path
        if thread_stop_event is not None:
            return not thread_stop_event.is_set() and any_work_to_do
        # If we are running in the main thread. This is a testing path
        else:
            return any_work_to_do

    def _schedule_forever(
            self, thread_stop_event: threading.Event,
            poll_interval: float = 10) -> None:
        sleep_time: float = 0.
        while not thread_stop_event.wait(timeout=sleep_time):
            poll_start = time.time()
            try:
                self.schedule(thread_stop_event)
            except Exception as e:
                logger.error(e)

            # compute how long to be idle
            time_since_last_poll = time.time() - poll_start
            if (poll_interval - time_since_last_poll) > 0:
                sleep_time = poll_interval - time_since_last_poll
            else:
                sleep_time = 0.

    def _log_executor_report_by(self) -> None:
        next_report_increment = (
            self.config.task_heartbeat_interval * self.config.report_by_buffer)

        try:
            errored_jobs = self.executor.get_errored_jobs()
        except NotImplementedError:
            logger.warning(f"{self.executor.__class__.__name__} does not implement "
                           f"errored_jobs methods.")
            errored_jobs = {}

        if errored_jobs:
            app_route = f'/scheduler/log_executor_error'
            return_code, response = shared_requester.send_request(
                app_route=app_route,
                message={'executor_ids': errored_jobs},
                request_type='post')
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected '
                    f'code 200. Response content: {response}'
                )

        try:
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warning(
                f"{self.executor.__class__.__name__} does not implement "
                "reconciliation methods. If a task instance does not "
                "register a heartbeat from a worker process in "
                f"{next_report_increment}s the task instance will be "
                "moved to error state.")
            actual = []
        if actual:
            app_route = (
                f'/scheduler/workflow_run/{self.workflow_run_id}/log_executor_report_by')
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={'executor_ids': actual,
                         'next_report_increment': next_report_increment},
                request_type='post')
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected '
                    f'code 200. Response content: {response}')

    def _log_workflow_run_heartbeat(self) -> None:
        next_report_increment = (
            self.config.task_heartbeat_interval * self.config.report_by_buffer)
        app_route = f"/scheduler/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'next_report_increment': next_report_increment},
            request_type='post')
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        status = response["message"]
        if status in [WorkflowRunStatus.COLD_RESUME,
                      WorkflowRunStatus.HOT_RESUME]:
            raise ResumeSet(f"Resume status ({status}) set by other agent.")
        elif status not in [WorkflowRunStatus.BOUND,
                            WorkflowRunStatus.RUNNING]:
            raise WorkflowRunStateError(
                f"Workflow run {self.workflow_run_id} tried to log a heartbeat"
                f" but was in state {status}. Workflow run must be in either "
                f"{WorkflowRunStatus.BOUND} or {WorkflowRunStatus.RUNNING}. "
                "Aborting execution.")

    def _get_tasks_queued_for_instantiation(self) -> List[ExecutorTask]:
        app_route = (
            f"/scheduler/workflow/{self.workflow_id}/queued_tasks/{self.config.n_queued}"
        )
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        tasks = [ExecutorTask.from_wire(j, self.executor.__class__.__name__,
                                        self.requester)
                 for j in response['task_dcts']]
        self._to_instantiate = tasks
        return tasks

    def _create_task_instance(self, task: ExecutorTask
                              ) -> Optional[ExecutorTaskInstance]:
        """
        Creates a TaskInstance based on the parameters of Task and tells the
        TaskStateManager to react accordingly.

        Args:
            task (ExecutorTask): A Task that we want to execute
        """
        try:
            task_instance = ExecutorTaskInstance.register_task_instance(
                task.task_id, self.workflow_run_id, self.executor,
                self.requester)
        except Exception as e:
            # we can't do anything more at this point so must return None
            logger.error(e)
            return None

        logger.debug("Executing {}".format(task.command))

        # TODO: unify qsub IDS to be meaningful across executor types
        command = task_instance.executor.build_wrapped_command(
            command=task.command,
            task_instance_id=task_instance.task_instance_id,
            heartbeat_interval=self.config.task_heartbeat_interval,
            report_by_buffer=self.config.report_by_buffer)
        # The following call will always return a value.
        # It catches exceptions internally and returns ERROR_SGE_JID
        logger.debug(
            "Using the following parameters in execution: "
            f"{task.executor_parameters}")
        executor_id = task_instance.executor.execute(
            command=command,
            name=task.name,
            executor_parameters=task.executor_parameters)
        if executor_id == qsub_attribute.NO_EXEC_ID:
            logger.debug(f"Received {executor_id} meaning "
                         f"the task did not qsub properly, moving "
                         f"to 'W' state")
            task_instance.register_no_executor_id(executor_id=executor_id)
        elif executor_id == qsub_attribute.UNPARSABLE:
            logger.debug(f"Got response from qsub but did not contain a "
                         f"valid executor_id. Using ({executor_id}), and "
                         f"moving to 'W' state")
            task_instance.register_no_executor_id(executor_id=executor_id)
        elif executor_id:
            report_by_buffer = (
                self.config.task_heartbeat_interval *
                self.config.report_by_buffer)
            task_instance.register_submission_to_batch_executor(
                executor_id, report_by_buffer)
        else:
            msg = ("Did not receive an executor_id in _create_task_instance")
            logger.error(msg)

        return task_instance

    def _get_lost_task_instances(self) -> None:
        app_route = (
            f'/scheduler/workflow_run/{self.workflow_run_id}/'
            'get_suspicious_task_instances')
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
        lost_task_instances = [
            ExecutorTaskInstance.from_wire(ti, self.executor, self.requester)
            for ti in response["task_instances"]]
        self._to_reconcile = lost_task_instances

    def _terminate_active_task_instances(self) -> None:
        app_route = (
            f'/scheduler/workflow_run/{self.workflow_run_id}/'
            'get_task_instances_to_terminate')
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')

        # eat bad responses here because we are outside of the exception
        # catching context
        if http_request_ok(return_code) is False:
            to_terminate: List = []
        else:
            to_terminate = [
                ExecutorTaskInstance.from_wire(
                    ti, self.executor, self.requester).executor_id
                for ti in response["task_instances"]]
        self.executor.terminate_task_instances(to_terminate)
