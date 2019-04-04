from http import HTTPStatus as StatusCodes
import logging
import threading
import _thread
from time import sleep
import traceback

from jobmon.client import shared_requester
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.job_instance_status import JobInstanceStatus


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 stop_event=None, requester=shared_requester):
        """The JobInstanceReconciler is a mechanism by which the
        JobStateManager and JobQueryServer make sure the database in sync with
        jobs in qstat

        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
            equentialExecutor, DummyExecutor or SGEExecutor
            interrupt_on_error (bool, default True): whether or not to
            interrupt the thread if there's an error
            stop_event (obj, default None): Object of type threading.Event
        """
        self.dag_id = dag_id
        self.requester = requester
        self.interrupt_on_error = interrupt_on_error

        if executor:
            self.set_executor(executor)
        else:
            se = SequentialExecutor()
            self.set_executor(se)

        if not stop_event:
            self._stop_event = threading.Event()
        else:
            self._stop_event = stop_event

    def set_executor(self, executor):
        """
        Sets the executor that will be used for all jobs queued downstream
        of the set event.

        Args:
            executor (callable): Any callable that takes a Job and returns
                either None or an Int. If Int is returned, this is assumed
                to be the JobInstances executor_id, and will be registered
                with the JobStateManager as such.
        """
        self.executor = executor

    def reconcile_periodically(self, poll_interval=10):
        """Running in a thread, this function allows the JobInstanceReconciler
        to periodically reconcile all jobs against 'qstat'

        Args:
            poll_interval (int): how often you want this function to poll for
            newly ready jobs
        """
        logger.info(
            f"Reconciling jobs against 'qstat' at {poll_interval}s intervals")
        while True and not self._stop_event.is_set():
            try:
                logging.debug(f"Reconciling at interval {poll_interval}s")
                self.reconcile()
                self.terminate_timed_out_jobs()
                sleep(poll_interval)
            except Exception as e:
                msg = f"About to raise Keyboard Interrupt signal {e}"
                logger.error(msg)
                stack = traceback.format_exc()
                logger.error(stack)
                # Also write to stdout because this is a serious problem
                print(msg)
                if self.interrupt_on_error:
                    _thread.interrupt_main()
                    self._stop_event.set()
                else:
                    raise

    def reconcile(self):
        """Identifies jobs that have disappeared from the batch execution
        system (e.g. SGE), and reports their disappearance back to the
        JobStateManager so they can either be retried or flagged as
        fatal errors
        """
        self._request_permission_to_reconcile()
        try:
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warning(f"{self.executor.__class__.__name__} does not "
                           "implement reconciliation methods")
            return []
        rc, response = self.requester.send_request(
            app_route=f'/task_dag/{self.dag_id}/reconcile',
            message={'executor_ids': actual},
            request_type='post')

    def terminate_timed_out_jobs(self):
        """Attempts to terminate jobs that have been in the "running"
        state for too long. From the SGE perspective, this might include
        jobs that got stuck in "r" state but never called back to the
        JobStateManager (i.e. SGE sees them as "r" but Jobmon sees them as
        SUBMITTED_TO_BATCH_EXECUTOR)
        """
        to_jobs = self._get_timed_out_jobs()
        try:
            terminated_job_instances = self.executor.terminate_job_instances(
                to_jobs)
            for ji_id, hostname in terminated_job_instances:
                self._log_timeout_error(int(ji_id))
                self._log_timeout_hostname(int(ji_id), hostname)
        except NotImplementedError:
            logger.warning("{} does not implement reconciliation methods"
                           .format(self.executor.__class__.__name__))

    def _get_timed_out_jobs(self):
        """Returns timed_out jobs as a list of JobInstances.

        TODO: Explore whether there is any utility in in a
        "from_wire_as_dataframe" utility method on JobInstance, similar to the
        current "from_wire" utility.
        """
        try:
            rc, response = self.requester.send_request(
                app_route=f'/dag/{self.dag_id}/job_instance_executor_ids',
                message={'status': [
                         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                         JobInstanceStatus.RUNNING],
                         'runtime': 'timed_out'},
                request_type='get')
            jiid_exid_tuples = response['jiid_exid_tuples']
            if rc != StatusCodes.OK:
                jiid_exid_tuples = []
        except TypeError:
            jiid_exid_tuples = []
        return jiid_exid_tuples

    def _log_timeout_hostname(self, job_instance_id, hostname):
        """Logs the hostname for any job that has timed out
        Args:
            job_instance_id (int): id for the job_instance that has timed out
            hostname (str): host where the job_instance was running
        """
        return self.requester.send_request(
            app_route='/job_instance/{}/log_nodename'.format(job_instance_id),
            message={'nodename': hostname},
            request_type='post')

    def _log_timeout_error(self, job_instance_id):
        """Logs if a job has timed out
        Args:
            job_instance_id (int): id for the job_instance that has timed out
        """
        return self.requester.send_request(
            app_route='/job_instance/{}/log_error'.format(job_instance_id),
            message={'error_message': "Timed out"},
            request_type='post')

    def _request_permission_to_reconcile(self):
        """Syncs with the database and logs a heartbeat"""
        return self.requester.send_request(
            app_route='/task_dag/{}/log_heartbeat'.format(self.dag_id),
            message={},
            request_type='post')
