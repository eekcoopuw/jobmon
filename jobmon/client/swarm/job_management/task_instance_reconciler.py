from http import HTTPStatus as StatusCodes
import threading
import _thread
from time import sleep
import traceback
from typing import Optional, List

from jobmon.client import shared_requester, client_config
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.client.swarm.job_management.executor_task_instance import (
    ExecutorTaskInstance)
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class TaskInstanceReconciler(object):
    """The TaskInstanceReconciler is a mechanism by which the
    JobStateManager and JobQueryServer make sure the database in sync with
    sge jobs in qstat

    Args:
        workflow_run_id (int): id of the workflow run the task instances are
            associated with
        executor (Executor, default SequentialExecutor): obj of type
            Executor
        stop_event (threading.Event, default None): stop signal
    """

    def __init__(self,
                 workflow_run_id: int,
                 executor: Optional[Executor] = None,
                 stop_event: Optional[threading.Event] = None,
                 requester: Requester = shared_requester) -> None:

        self.workflow_run_id = workflow_run_id
        self.requester = requester
        self.reconciliation_interval = client_config.reconciliation_interval
        self.report_by_buffer = client_config.report_by_buffer
        self.heartbeat_interval = client_config.heartbeat_interval

        if executor:
            self.set_executor(executor)
        else:
            se = SequentialExecutor()
            self.set_executor(se)

        if not stop_event:
            self._stop_event = threading.Event()
        else:
            self._stop_event = stop_event

    def set_executor(self, executor: Executor) -> None:
        """
        Sets the executor that will be used for all tasks queued downstream
        of the set event.

        Args:
            executor (Executor): Any callable that takes a Task and returns
                either None or an Int. If Int is returned, this is assumed
                to be the TaskInstances executor_id, and will be registered
                with the JobStateManager as such.
        """
        self.executor = executor

    def reconcile_periodically(self) -> None:
        """Running in a thread, this function allows the TaskInstanceReconciler
        to periodically reconcile all tasks against 'qstat'
        """
        logger.info(
            f"Reconciling tasks against 'qstat' at "
            f"{self.reconciliation_interval} intervals and updating the report"
            f" by date at {self.report_by_buffer} times the heartbeat "
            f"interval: {self.heartbeat_interval}")

        while True and not self._stop_event.is_set():
            try:
                logger.debug(
                    f"Reconciling at interval {self.reconciliation_interval}s")
                # if your executor has the possiblity of timed out jobs still
                # running, terminate them here
                self.reconcile()
                sleep(self.reconciliation_interval)
            except Exception as e:
                msg = f"About to raise Keyboard Interrupt signal {e}"
                logger.error(msg)
                stack = traceback.format_exc()
                logger.error(stack)
                # Also write to stdout because this is a serious problem
                print(msg, stack)
                # Also send to server
                msg = (
                    f"Error in {self.__class__.__name__}, {str(self)} "
                    f"in reconcile_periodically: \n{stack}")
                shared_requester.send_request(
                    app_route="/error_logger",
                    message={"traceback": msg},
                    request_type="post")
                _thread.interrupt_main()
                self._stop_event.set()
                raise

    def terminate_timed_out_tasks(self) -> None:
        """
        If the executor does not automatically kill timed out tasks, make sure
        they have been terminated using this function
        """
        try:
            rc, response = self.requester.send_request(
                app_route=f'/workflow_run/{self.workflow_run_id}'
                f'/get_timed_out_executor_ids',
                message={},
                request_type='get')
            to_tasks = response['tiid_exid_tuples']
            if rc != StatusCodes.OK:
                to_tasks = []
        except TypeError:
            to_tasks = []
        try:
            terminated = self.executor.terminate_task_instances(to_tasks)
        except NotImplementedError:
            logger.warning("{} does not implement reconciliation methods"
                           .format(self.executor.__class__.__name__))
            terminated = []

        # log the hostname in case we need to terminate it remotely later
        for task_instance_id, hostname in terminated:
            self._log_timeout_hostname(task_instance_id, hostname)

    def reconcile(self) -> None:
        """Identifies submitted to batch and running tasks that have missed
        their report_by_date and reports their disappearance back to the
        JobStateManager so they can either be retried or flagged as
        fatal errors
        """
        self._log_workflow_run_heartbeat()
        self._log_executor_report_by()
        self._account_for_lost_task_instances()

    def _log_workflow_run_heartbeat(self) -> None:
        """Logs a dag heartbeat"""
        return self.requester.send_request(
            app_route=f'/workflow_run/{self.workflow_run_id}/log_heartbeat',
            message={},
            request_type='post')

    def _log_executor_report_by(self) -> None:
        next_report_increment = self.heartbeat_interval * self.report_by_buffer
        try:
            # qstat for pending jobs or running jobs
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warning(
                f"{self.executor.__class__.__name__} does not implement "
                f"reconciliation methods. If a task instance does not register"
                f" a heartbeat from a worker process in "
                f"{next_report_increment}s the task instance will be moved to "
                f"error state.")
            actual = []
        rc, response = self.requester.send_request(
            app_route=f'/task_instance/log_executor_report_by',
            message={'executor_ids': actual,
                     'next_report_increment': next_report_increment},
            request_type='post')

    def _account_for_lost_task_instances(self) -> None:
        rc, response = self.requester.send_request(
            app_route=f'/worklfow_run/{self.workflow_run_id}'
            f'/get_suspicious_task_instances',
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            lost_task_instances: List[ExecutorTaskInstance] = []
        else:
            lost_task_instances = [
                ExecutorTaskInstance.from_wire(ti, self.executor)
                for ti in response["task_instances"]
            ]
        for executor_task_instance in lost_task_instances:
            executor_task_instance.log_error()

    def _log_timeout_hostname(self, task_instance_id: int, hostname: str
                              ) -> None:
        """Logs the hostname for any job that has timed out
        Args:
            task_instance_id (int): id for the task_instance that has timed out
            hostname (str): host where the job was running
        """
        logger.info(f"log timeout hostname tiid: {task_instance_id} "
                    f"host: {hostname}")
        return self.requester.send_request(
            app_route=f'/task_instance/{task_instance_id}/log_nodename',
            message={'nodename': hostname},
            request_type='post')
