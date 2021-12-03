"""Distributes and monitors state of Task Instances."""
from __future__ import annotations

import logging
import multiprocessing as mp
import sys
import threading
import time
from types import TracebackType
from typing import Dict, List, Optional, Type

import tblib.pickling_support

from jobmon.client.client_logging import ClientLogging
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.exceptions import (
    InvalidResponse,
    RemoteExitInfoNotAvailable,
    ResumeSet,
    WorkflowRunStateError,
)
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeClusterType

ClientLogging().attach(__name__)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
tblib.pickling_support.install()


class ExceptionWrapper(object):
    """Handle exceptions."""

    def __init__(self, ee: Exception) -> None:
        """Initialization of execution wrapper."""
        self.ee = ee
        self.type: Optional[Type[BaseException]]
        self.value: Optional[BaseException]
        self.tb: Optional[TracebackType]
        self.type, self.value, self.tb = sys.exc_info()

    def re_raise(self) -> None:
        """Raise errors and add their traceback."""
        raise self.ee.with_traceback(self.tb)


class DistributorService:
    def __init__(
        self,
        workflow_id: int,
        workflow_run_id: int,
        distributor: ClusterDistributor,
        requester: Requester,
        wf_max_concurrently_running: int,
        workflow_run_heartbeat_interval: int = 30,
        task_instance_heartbeat_interval: int = 90,
        heartbeat_report_by_buffer: float = 3.1,
        n_queued: int = 100,
        distributor_poll_interval: int = 10,
        worker_node_entry_point: Optional[str] = None
    ) -> None:
        """Initialization of distributor service."""
        # which workflow to distribute for
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.wf_max_concurrently_running = wf_max_concurrently_running

        # cluster_name
        self.distributor = distributor

        # operational args
        self._worker_node_entry_point = worker_node_entry_point
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self._report_by_buffer = heartbeat_report_by_buffer
        self._n_queued = n_queued
        self._distributor_poll_interval = distributor_poll_interval

        self.requester = requester

        self.distributor_wfr = DistributorWorkflowRun(self.workflow_id, self.workflow_run_id,
                                                      self.requester)

        logger.info(f"distributor communicating at {self.requester.url}")

        # Get/set cluster_type_id
        self._get_cluster_type_id()

        # state tracking
        # Move workflow and workflow run to Instantiating
        self._instantiate_workflows()

        self._submitted_or_running: Dict[int, DistributorTaskInstance] = {}
        self._to_instantiate: List[DistributorTask] = []
        self._to_launch_single_tis: List[DistributorTask] = []
        self._to_launch_array_tis: List[DistributorTask] = []
        self._to_reconcile: List[DistributorTaskInstance] = []
        self._to_log_error: List[DistributorTaskInstance] = []

        # Move workflow and workflow run to launched
        self._launch_workflows()

        # log heartbeat on startup so workflow run FSM doesn't have any races
        self.heartbeat()

    def _get_cluster_type_id(self) -> None:
        """Get the cluster_type_id associated with the cluster_type_name."""
        app_route = f"/cluster_type/{self.distributor.cluster_type_name}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from GET "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )
        cluster_type_kwargs = SerializeClusterType.kwargs_from_wire(
            response["cluster_type"]
        )
        self.cluster_type_id = cluster_type_kwargs["id"]

    def _infer_error(self, task_instance: DistributorTaskInstance) -> None:
        """Infer error by checking the distributor remote exit info."""
        # infer error state if we don't know it already
        if task_instance.distributor_id is None:
            raise ValueError("distributor_id cannot be None during log_error")
        distributor_id = task_instance.distributor_id

        try:
            error_state, error_msg = self.distributor.get_remote_exit_info(
                distributor_id
            )
        except RemoteExitInfoNotAvailable:
            error_state = TaskInstanceStatus.UNKNOWN_ERROR
            error_msg = (
                f"Unknown error caused task_instance_id "
                f"{task_instance.task_instance_id} to be lost"
            )
        task_instance.error_state = error_state
        task_instance.error_msg = error_msg

    def _instantiate_workflows(self) -> None:
        """Move the workflow and workflow run to instantiating."""
        # Update workflow run
        wfr_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        rc, resp = self.requester.send_request(
            app_route=wfr_route,
            message={"status": WorkflowRunStatus.INSTANTIATING},
            request_type="put",
            logger=logger,
        )

        if http_request_ok(rc) is False:
            raise InvalidResponse(
                f"Unexpected status code {rc} from PUT "
                f"request through route {wfr_route}. Expected "
                f"code 200. Response content: {resp}"
            )

    def _launch_workflows(self) -> None:
        """Move the workflow and workflow run to launched."""
        # Update workflow run
        wfr_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        rc, resp = self.requester.send_request(
            app_route=wfr_route,
            message={"status": WorkflowRunStatus.LAUNCHED},
            request_type="put",
            logger=logger,
        )

        if http_request_ok(rc) is False:
            raise InvalidResponse(
                f"Unexpected status code {rc} from PUT "
                f"request through route {wfr_route}. Expected "
                f"code 200. Response content: {resp}"
            )

    def run_distributor(
        self,
        stop_event: Optional[mp.synchronize.Event] = None,
        status_queue: Optional[mp.Queue] = None,
    ) -> None:
        """Start up the distributor."""
        try:
            # start up the worker thread and distributor
            self.distributor.start()
            logger.info("Distributor has started")

            # send response back to main
            if status_queue is not None:
                status_queue.put("ALIVE")

            # work loop is always running in a separate thread
            thread_stop_event = threading.Event()
            thread = threading.Thread(
                target=self._distribute_forever,
                args=(thread_stop_event, self._distributor_poll_interval),
            )
            thread.daemon = True
            thread.start()

            # infinite blocking loop unless resume or stop requested from
            # main process
            self._heartbeats_forever(self._workflow_run_heartbeat_interval, stop_event)

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

            # terminate jobs via distributor API
            self._terminate_active_task_instances()

            # send error back to main
            if status_queue is not None:
                logger.warning(f"Termination complete. Returning {e} to main thread.")
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
            # stop distributor
            self.distributor.stop(
                distributor_ids=list(self._submitted_or_running.keys())
            )

            if status_queue is not None:
                status_queue.put("SHUTDOWN")

    def heartbeat(self) -> None:
        """Log heartbeats to notify that distributor, therefore workflow run is still alive."""
        # log heartbeats for tasks queued for batch distributor and for the
        # workflow run
        logger.debug("distributor: logging heartbeat")
        self._purge_queueing_errors()
        self._log_distributor_report_by()
        self._log_workflow_run_heartbeat()

    def distribute(self, thread_stop_event: Optional[threading.Event] = None) -> None:
        """Distribute and reconcile on an interval."""
        logger.info(
            "Distributing work. Reconciling queue discrepancies. Logging errors."
        )

        # get work if there isn't any in the queues
        if not self._to_instantiate and not self._to_reconcile:
            self._get_tasks_queued_for_instantiation()
            logger.debug(f"Found {len(self._to_instantiate)} Queued Tasks")
            self._get_lost_task_instances()
            logger.debug(f"Found {len(self._to_reconcile)} Lost Tasks")

        # iterate through all work to do unless a stop event is set from the
        # main thread
        while self._keep_distributing(thread_stop_event):

            # instantiate queued tasks
            if self._to_instantiate:
                task = self._to_instantiate.pop(0)
                self._create_task_instance(task)

            # infer errors and move from reconciliation queue to error queue
            if self._to_reconcile:
                task_instance = self._to_reconcile.pop(0)
                self._infer_error(task_instance)
                self._to_log_error.append(task_instance)

        # log all errors
        while self._to_log_error:
            task_instance = self._to_log_error.pop(0)
            task_instance.log_error()

    def _heartbeats_forever(
        self,
        heartbeat_interval: int = 90,
        process_stop_event: Optional[mp.synchronize.Event] = None,
    ) -> None:
        keep_beating = True
        while keep_beating:
            self.heartbeat()

            # check if we need to interrupt
            if process_stop_event is not None:
                if process_stop_event.wait(timeout=heartbeat_interval):
                    keep_beating = False
            else:
                time.sleep(heartbeat_interval)

    def _keep_distributing(
        self, thread_stop_event: Optional[threading.Event] = None
    ) -> bool:
        any_work_to_do = any(self._to_instantiate) or any(self._to_reconcile)
        # If we are running in a thread. This is the standard path

        if thread_stop_event is not None:
            res = not thread_stop_event.is_set() and any_work_to_do
        # If we are running in the main thread. This is a testing path
        else:
            res = any_work_to_do
        logger.info(f"keep distributing is {res}")
        return res

    def _distribute_forever(
        self, thread_stop_event: threading.Event, poll_interval: float = 10
    ) -> None:
        sleep_time: float = 0.0
        while not thread_stop_event.wait(timeout=sleep_time):
            poll_start = time.time()
            try:
                self.distribute(thread_stop_event)
            except Exception as e:
                logger.error(e)

            # compute how long to be idle
            time_since_last_poll = time.time() - poll_start
            if (poll_interval - time_since_last_poll) > 0:
                sleep_time = poll_interval - time_since_last_poll
            else:
                sleep_time = 0.0

    def _purge_queueing_errors(self) -> None:
        """Remove any jobs that have encountered an error in the distributor queue."""
        active_distributor_ids = list(self._submitted_or_running.keys())
        try:
            # get jobs that encountered a queueing error and terminate them
            distributor_errors = self.distributor.get_queueing_errors(
                active_distributor_ids
            )
            if distributor_errors:
                self.distributor.terminate_task_instances(
                    list(distributor_errors.keys())
                )
                logger.debug(f"errored_jobs: {distributor_errors}")

            # store error message and handle in distributing thread
            for distributor_id, msg in distributor_errors.items():
                task_instance = self._submitted_or_running.pop(distributor_id)
                task_instance.error_state = TaskInstanceStatus.ERROR_FATAL
                task_instance.error_msg = msg
                self._to_log_error.append(task_instance)

        except NotImplementedError:
            logger.warning(
                f"{self.distributor.__class__.__name__} does not implement "
                f"get_errored_jobs methods."
            )
        except Exception as e:
            logger.warning(str(e))

    def _log_distributor_report_by(self) -> None:
        next_report_increment = (
            self._task_instance_heartbeat_interval * self._report_by_buffer
        )
        active_distributor_ids = list(self._submitted_or_running.keys())

        try:
            actual = self.distributor.get_submitted_or_running(active_distributor_ids)
            logger.debug(f"active distributor_ids: {actual}")
        except NotImplementedError:
            logger.warning(
                f"{self.distributor.__class__.__name__} does not implement "
                "reconciliation methods. If a task instance does not "
                "register a heartbeat from a worker process in "
                f"{next_report_increment}s the task instance will be "
                "moved to error state."
            )
            actual = []

        # log heartbeat in the database and locally here in the distributor
        if actual:
            app_route = (
                f"/workflow_run/{self.workflow_run_id}/log_distributor_report_by"
            )
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={
                    "distributor_ids": actual,
                    "next_report_increment": next_report_increment,
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

            new_report_by_date = time.time() + next_report_increment
            for distributor_id in actual:
                executing_task_instance = self._submitted_or_running.get(distributor_id)
                if executing_task_instance is not None:
                    executing_task_instance.report_by_date = new_report_by_date
                else:
                    logger.warning(
                        f"distributor_id {distributor_id} found in qstat but not in"
                        " distributor tracking for submitted or running tasks"
                    )

        # remove task instance from tracking if they haven't logged a heartbeat in a while
        current_time = time.time()
        disappeared_distributor_ids = set(active_distributor_ids) - set(actual)
        for distributor_id in disappeared_distributor_ids:
            miss_task_instance = self._submitted_or_running[distributor_id]
            if miss_task_instance.report_by_date > current_time:
                del self._submitted_or_running[distributor_id]

    def _log_workflow_run_heartbeat(self) -> None:
        next_report_increment = (
            self._task_instance_heartbeat_interval * self._report_by_buffer
        )
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "next_report_increment": next_report_increment,
                "status": WorkflowRunStatus.RUNNING,
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

        status = response["message"]
        if status in [WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME]:
            raise ResumeSet(f"Resume status ({status}) set by other agent.")
        elif status not in [WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.RUNNING]:
            raise WorkflowRunStateError(
                f"Workflow run {self.workflow_run_id} tried to log a heartbeat"
                f" but was in state {status}. Workflow run must be in either "
                f"{WorkflowRunStatus.LAUNCHED} or {WorkflowRunStatus.RUNNING}. "
                "Aborting distributor."
            )

    def _get_tasks_queued_for_instantiation(self) -> List[DistributorTask]:
        app_route = f"/workflow/{self.workflow_id}/queued_tasks/{self._n_queued}"
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
            DistributorTask.from_wire(
                t, self.distributor.__class__.__name__, self.requester
            )
            for t in response["task_dcts"]
        ]
        self._to_instantiate = tasks
        return tasks

    def _create_task_instance(
        self, task: DistributorTask
    ) -> Optional[DistributorTaskInstance]:
        """Creates a TaskInstance based on the parameters of Task.

        Tells the TaskStateManager to react accordingly.

        Args:
            task (DistributorTask): A Task that we want to execute
        """
        task_instance = DistributorTaskInstance.register_task_instance(
            task.task_id,
            self.workflow_run_id,
            self.cluster_type_id,
            self.requester,
        )
        logger.debug("Executing {}".format(task.command))
        command = self.distributor.build_worker_node_command(
            task_instance.task_instance_id
        )

        try:
            logger.debug(
                f"Using the following resources in execution {task.requested_resources}"
            )
            distributor_id = self.distributor.submit_to_batch_distributor(
                command=command,
                name=task.name,
                requested_resources=task.requested_resources,
            )
        except Exception as e:
            task_instance.register_no_distributor_id(no_id_err_msg=str(e))
        else:
            report_by_buffer = (
                self._task_instance_heartbeat_interval * self._report_by_buffer
            )
            task_instance.register_submission_to_batch_distributor(
                distributor_id, report_by_buffer
            )
            self._submitted_or_running[distributor_id] = task_instance

        return task_instance

    def _get_lost_task_instances(self) -> None:
        app_route = (
            f"/workflow_run/{self.workflow_run_id}/get_suspicious_task_instances"
        )

        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        lost_task_instances = [
            DistributorTaskInstance.from_wire(ti, self.cluster_type_id, self.requester)
            for ti in response["task_instances"]
        ]
        self._to_reconcile = lost_task_instances
        logger.debug(f"Jobs to be reconciled: {self._to_reconcile}")

    def _terminate_active_task_instances(self) -> None:
        app_route = (
            f"/workflow_run/{self.workflow_run_id}/get_task_instances_to_terminate"
        )
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get", logger=logger
        )

        # eat bad responses here because we are outside of the exception
        # catching context
        if http_request_ok(return_code) is False:
            to_terminate: List = []
        else:
            to_terminate = [
                DistributorTaskInstance.from_wire(
                    ti, self.cluster_type_id, self.requester
                ).distributor_id
                for ti in response["task_instances"]
            ]
        self.distributor.terminate_task_instances(to_terminate)

    def launch_array_task_instances(self, array, array_instantiated_tiids: List[int]) -> List[int]:
        # Take the lower concurrency limit (since array limit should never be greater than
        # workflow limit).
        concurrency_limit = min(array.max_concurrently_running, self.wf_max_concurrently_running)

        # Calculate total launched and running task instances in workflow
        running_launched_tasks = len(self.distributor_wfr.launched_task_instance) + \
                                 len(self.distributor_wfr.running_task_instances)

        # Calculate total tasks running and launched in Array
        running_launched_array_tasks = len(self.distributor_wfr.launched_array_task_instances) + \
                                       len(self.distributor_wfr.running_array_task_instances)

        array_capacity = concurrency_limit - running_launched_array_tasks
        workflow_capacity = concurrency_limit - running_launched_tasks

        # Take the lower capacity amount
        capacity = min(array_capacity, workflow_capacity)
        array.instantiated_array_task_instance_ids = array_instantiated_tiids[:capacity]

        # launch array
        self.distributor_wfr.launched_array_task_instances(array, self.distributor)

        # return a list of the task instance ids that were launched
        return array.instantiated_array_task_instance_ids

    def launch_task_instances(self, instantiated_tiids: List[int]) -> List[int]:

        # Calculate total launched and running task instances in workflow
        running_launched_tasks = len(self.distributor_wfr.launched_task_instance) + \
                                 len(self.distributor_wfr.running_task_instances)

        # Calculate wf capacity (max_concurrently_running - (running and launched))
        workflow_capacity = self.wf_max_concurrently_running - running_launched_tasks

        for ti in instantiated_tiids[:workflow_capacity]:
            self.distributor_wfr.launched_task_instance(ti, self.distributor)

        # return a list of the task instance ids that were launched
        return instantiated_tiids[:workflow_capacity]
