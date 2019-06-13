from http import HTTPStatus as StatusCodes
import logging
import threading
import _thread
from time import sleep
import traceback
from typing import Optional, List

from jobmon.client import shared_requester, client_config
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.client.swarm.job_management.executor_job_instance import (
    ExecutorJobInstance)


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):
    """The JobInstanceReconciler is a mechanism by which the
    JobStateManager and JobQueryServer make sure the database in sync with
    jobs in qstat

    Args:
        dag_id (int): the id for the dag to run
        executor (Executor, default SequentialExecutor): obj of type
            Executor
        interrupt_on_error (bool, default True): whether or not to
            interrupt the thread if there's an error
        stop_event (threading.Event, default None): stop signal
    """

    def __init__(self,
                 dag_id: int,
                 executor: Optional[Executor] = None,
                 interrupt_on_error: Optional[bool] = True,
                 stop_event: Optional[threading.Event] = None,
                 requester: Requester = shared_requester) -> None:

        self.dag_id = dag_id
        self.requester = requester
        self.interrupt_on_error = interrupt_on_error
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
        Sets the executor that will be used for all jobs queued downstream
        of the set event.

        Args:
            executor (Executor): Any callable that takes a Job and returns
                either None or an Int. If Int is returned, this is assumed
                to be the JobInstances executor_id, and will be registered
                with the JobStateManager as such.
        """
        self.executor = executor

    def reconcile_periodically(self) -> None:
        """Running in a thread, this function allows the JobInstanceReconciler
        to periodically reconcile all jobs against 'qstat'
        """
        logger.info(
            f"Reconciling jobs against 'qstat' at "
            f"{self.reconciliation_interval} intervals and updating the report"
            f" by date at {self.report_by_buffer} times the heartbeat "
            f"interval: {self.heartbeat_interval}")

        while True and not self._stop_event.is_set():
            try:
                logging.debug(
                    f"Reconciling at interval {self.reconciliation_interval}s")
                self.terminate_timed_out_jobs()
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
                if self.interrupt_on_error:
                    _thread.interrupt_main()
                    self._stop_event.set()
                else:
                    raise

    def terminate_timed_out_jobs(self) -> None:
        """Attempts to terminate jobs that have been in the "running"
        state for too long. From the SGE perspective, this might include
        jobs that got stuck in "r" state but never called back to the
        JobStateManager (i.e. SGE sees them as "r" but Jobmon sees them as
        SUBMITTED_TO_BATCH_EXECUTOR)
        """
        try:
            rc, response = self.requester.send_request(
                app_route=f'/dag/{self.dag_id}/get_timed_out_executor_ids',
                message={},
                request_type='get')
            to_jobs = response['jiid_exid_tuples']
            if rc != StatusCodes.OK:
                to_jobs = []
        except TypeError:
            to_jobs = []
        try:
            terminated = self.executor.terminate_job_instances(to_jobs)
        except NotImplementedError:
            logger.warning("{} does not implement reconciliation methods"
                           .format(self.executor.__class__.__name__))
            terminated = []

        # log the hostname in case we need to terminate it remotely later
        for job_instance_id, hostname in terminated:
            self._log_timeout_hostname(job_instance_id, hostname)

    def reconcile(self) -> None:
        """Identifies submitted to batch and running jobs that have missed
        their report_by_date and reports their disappearance back to the
        JobStateManager so they can either be retried or flagged as
        fatal errors
        """
        self._log_dag_heartbeat()
        self._log_executor_report_by()
        self._account_for_lost_job_instances()

    def _log_dag_heartbeat(self) -> None:
        """Logs a dag heartbeat"""
        return self.requester.send_request(
            app_route='/task_dag/{}/log_heartbeat'.format(self.dag_id),
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
                "reconciliation methods. If a job instance does not register a"
                " heartbeat from a worker process in "
                f"{next_report_increment}s the job instance will be "
                "moved to error state.")
            actual = []
        rc, response = self.requester.send_request(
            app_route=f'/task_dag/{self.dag_id}/log_executor_report_by',
            message={'executor_ids': actual,
                     'next_report_increment': next_report_increment},
            request_type='post')

    def _account_for_lost_job_instances(self) -> None:
        rc, response = self.requester.send_request(
            app_route=f'/dag/{self.dag_id}/get_suspicious_job_instances',
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            lost_job_instances: List[ExecutorJobInstance] = []
        else:
            lost_job_instances = [
                ExecutorJobInstance.from_wire(ji, self.executor)
                for ji in response["job_instances"]
            ]
        for executor_job_instance in lost_job_instances:
            executor_job_instance.log_error()

    def _log_timeout_hostname(self, job_instance_id: int, hostname: str):
        """Logs the hostname for any job that has timed out
        Args:
            job_instance_id (int): id for the job_instance that has timed out
            hostname (str): host where the job_instance was running
        """
        return self.requester.send_request(
            app_route=f'/job_instance/{job_instance_id}/log_nodename',
            message={'nodename': hostname},
            request_type='post')
