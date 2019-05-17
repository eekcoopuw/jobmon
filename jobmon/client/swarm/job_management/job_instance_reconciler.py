import datetime
from http import HTTPStatus as StatusCodes
import logging
import threading
import _thread
from time import sleep
import traceback

from jobmon.client import shared_requester, client_config
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.job_instance_status import JobInstanceStatus


logger = logging.getLogger(__name__)

DEFAULT_INCREMENT_SCALE = 0.5


class LostJobInstance:

    def __init__(self, job_instance_id, executor_id, executor,
                 workflow_run_id=None, nodename=None, process_group_id=None,
                 job_id=None, dag_id=None, status=None, status_date=None):

        # job_instance_id should be immutable so make it private
        self.job_instance_id = job_instance_id
        self.executor_id = executor_id

        # interface to the executor
        self.executor = executor
        self._exit_status = None
        self._msg = None

        # these attributes do not affect identity/set equality
        self.workflow_run_id = workflow_run_id
        self.nodename = nodename
        self.process_group_id = process_group_id
        self.job_id = job_id
        self.dag_id = dag_id
        self.status = status
        self.status_date = status_date

    @classmethod
    def from_wire(cls, dct, executor):
        return cls(job_instance_id=dct['job_instance_id'],
                   executor=executor,
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   nodename=dct['nodename'],
                   process_group_id=dct['process_group_id'],
                   job_id=dct['job_id'],
                   dag_id=dct['dag_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def time_since_status(self):
        return (datetime.utcnow() - self.status_date).seconds

    def log_error(self):
        error_state, msg = self.executor.get_exit_info(self.executor_id)

        # this is the 'happy' path. The executor gives us a concrete error for
        # the lost job
        if error_state == JobInstanceStatus.RESOURCE_ERROR:
            self.state = error_state
            message = {
                "error_message": msg,
                "error_state": self.state
            }
        # this is the 'unhappy' path. We are giving up discovering the exit
        # state and moving the job into unknown error state
        elif self.time_since_status > client_config.lost_track_timeout:
            self.status = error_state
            message = {
                "error_message": msg,
                "error_state": self.state
            }
        # this is the do nothing path. We want to wait longer to see if the
        # executor discovers the real exit state before giving up on the job
        else:
            return
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_error',
            message=message,
            request_type='post')


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
        self.reconciliation_interval = client_config.reconciliation_interval
        self.report_by_buffer = client_config.report_by_buffer
        self.heartbeat_interval = client_config.heartbeat_interval
        self.lost_job_instances = []

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

    def reconcile_periodically(self):
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

    def terminate_timed_out_jobs(self):
        """Attempts to terminate jobs that have been in the "running"
        state for too long. From the SGE perspective, this might include
        jobs that got stuck in "r" state but never called back to the
        JobStateManager (i.e. SGE sees them as "r" but Jobmon sees them as
        SUBMITTED_TO_BATCH_EXECUTOR)
        """
        try:
            rc, response = self.requester.send_request(
                app_route=f'/dag/{self.dag_id}/get_timed_out_job_instances',
                message={'status':
                         [],
                         'runtime': 'timed_out'},
                request_type='get')
            to_jobs = response['jiid_exid_tuples']
            if rc != StatusCodes.OK:
                to_jobs = []
        except TypeError:
            to_jobs = []
        try:
            self.executor.terminate_job_instances(to_jobs)
        except NotImplementedError:
            logger.warning("{} does not implement reconciliation methods"
                           .format(self.executor.__class__.__name__))

    def reconcile(self):
        """Identifies submitted to batch and running jobs that have missed
        their report_by_date and reports their disappearance back to the
        JobStateManager so they can either be retried or flagged as
        fatal errors
        """
        self._log_dag_heartbeat()
        self._log_executor_report_by()
        self._transition_job_instances_to_lost()
        self._update_lost_job_instances()
        self._account_for_lost_job_instances()

    def _log_dag_heartbeat(self):
        """Logs a dag heartbeat"""
        return self.requester.send_request(
            app_route='/task_dag/{}/log_heartbeat'.format(self.dag_id),
            message={},
            request_type='post')

    def _log_executor_report_by(self):
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

    def _transition_job_instances_to_lost(self):
        return self.requester.send_request(
            app_route=f'/task_dag/{self.dag_id}/transition_jis_to_lost',
            message={},
            request_type='post')

    def _get_lost_job_instances(self):
        rc, response = self.requester.send_request(
            app_route=f'/task_dag/{self.dag_id}/job_instances_by_status',
            message={"status": [JobInstanceStatus.LOST_TRACK]},
            request_type='post')
        if rc != StatusCodes.OK:
            self._lost_job_instances = []
        else:
            self._lost_job_instances = [
                LostJobInstance.from_wire(ji, self.executor)
                for ji in response["job_instances"]
            ]

    def _account_for_lost_job_instances(self):
        for lost_job_instance in self._lost_job_instances:
            lost_job_instance.log_error()

    # def terminate_timed_out_jobs(self):
    #     """Attempts to terminate jobs that have been in the "running"
    #     state for too long. From the SGE perspective, this might include
    #     jobs that got stuck in "r" state but never called back to the
    #     JobStateManager (i.e. SGE sees them as "r" but Jobmon sees them as
    #     SUBMITTED_TO_BATCH_EXECUTOR)
    #     """
    #     to_jobs = self._get_timed_out_jobs()
    #     try:
    #         terminated_job_instances = self.executor.terminate_job_instances(
    #             to_jobs)
    #         for ji_id, hostname in terminated_job_instances:
    #             self._log_timeout_error(int(ji_id))
    #             self._log_timeout_hostname(int(ji_id), hostname)
    #     except NotImplementedError:
    #         logger.warning("{} does not implement reconciliation methods"
    #                        .format(self.executor.__class__.__name__))

    # def _get_timed_out_jobs(self):
    #     """Returns timed_out jobs as a list of JobInstances.
    #     """
    #     try:
    #         rc, response = self.requester.send_request(
    #             app_route=f'/dag/{self.dag_id}/job_instance_executor_ids',
    #             message={'status': [
    #                      JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
    #                      JobInstanceStatus.RUNNING],
    #                      'runtime': 'timed_out'},
    #             request_type='get')
    #         jiid_exid_tuples = response['jiid_exid_tuples']
    #         if rc != StatusCodes.OK:
    #             jiid_exid_tuples = []
    #     except TypeError:
    #         jiid_exid_tuples = []
    #     return jiid_exid_tuples

    # def _log_timeout_hostname(self, job_instance_id, hostname):
    #     """Logs the hostname for any job that has timed out
    #     Args:
    #         job_instance_id (int): id for the job_instance that has timed out
    #         hostname (str): host where the job_instance was running
    #     """
    #     return self.requester.send_request(
    #         app_route='/job_instance/{}/log_nodename'.format(job_instance_id),
    #         message={'nodename': hostname},
    #         request_type='post')

    # def _log_timeout_error(self, job_instance_id, executor_id=None):
    #     """Logs if a job has timed out
    #     Args:
    #         job_instance_id (int): id for the job_instance that has timed out
    #     """
    #     message = {'error_message': "Timed out", 'nodename': socket.getfqdn()}
    #     if executor_id is not None:
    #         message['executor_id'] = executor_id
    #     exit_code = qacct_exit_status(executor_id)
    #     message['exit_status'] = exit_code
    #     return self.requester.send_request(
    #         app_route='/job_instance/{}/log_error'.format(job_instance_id),
    #         message=message,
    #         request_type='post',
    #         )
