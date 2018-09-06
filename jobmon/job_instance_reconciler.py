import logging
import threading
import _thread
from time import sleep
import traceback

from zmq.error import ZMQError

from jobmon.config import config
from jobmon.exceptions import ReturnCodes
from jobmon.executors.sequential import SequentialExecutor
from jobmon.models import JobInstance
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 stop_event=None):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)
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
        logger.info("Reconciling jobs against 'qstat' at {}s "
                    "intervals".format(poll_interval))
        while True and not self._stop_event.is_set():
            try:
                logging.debug(
                    "Reconciling at interval {}s".format(poll_interval))
                self.reconcile()
                self.terminate_timed_out_jobs()
                sleep(poll_interval)
            except ZMQError as e:
                # Tests rely on some funky usage of various REQ/REP pairs
                # across threads, so interrupting here can be problematic...

                # ... since this interrupt is primarily in reponse to potential
                # SGE failures anyways, I'm just going to warn for now on ZMQ
                # errors and save the interrupts for everything else
                logger.warning(e)
            except Exception as e:
                msg = "About to raise Keyboard Interrupt signal {}".format(e)
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
        fatal errors"""
        presumed = self._get_presumed_submitted_or_running()
        self._request_permission_to_reconcile()
        try:
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warn("{} does not implement reconciliation methods".format(
                self.executor.__class__.__name__))
            return []

        # This is kludgy... Re-visit the data structure used for communicating
        # executor IDs back from the JobQueryServer
        missing_job_instance_ids = []
        for job_instance in presumed:
            if job_instance.executor_id:
                if job_instance.executor_id not in actual:
                    job_instance_id = job_instance.job_instance_id
                    self._log_mysterious_error(job_instance_id,
                                               job_instance.executor_id)
                    missing_job_instance_ids.append(job_instance_id)
        return missing_job_instance_ids

    def terminate_timed_out_jobs(self):
        """Attempts to terminate jobs that have been in the "running"
        state for too long. From the SGE perspective, this might include
        jobs that got stuck in "r" state but never called back to the
        JobStateManager (i.e. SGE sees them as "r" but Jobmon sees them as
        SUBMITTED_TO_BATCH_EXECUTOR)"""
        to_jobs = self._get_timed_out_jobs()
        try:
            terminated_job_instances = self.executor.terminate_job_instances(
                to_jobs)
            for ji_id, hostname in terminated_job_instances:
                self._log_timeout_error(int(ji_id))
                self._log_timeout_hostname(int(ji_id), hostname)
        except NotImplementedError:
            logger.warn("{} does not implement reconciliation methods".format(
                self.executor.__class__.__name__))

    def _get_presumed_submitted_or_running(self):
        try:
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_submitted_or_running',
                'kwargs': {'dag_id': self.dag_id}
            })
            job_instances = [JobInstance.from_wire(j) for j in job_instances]
        except TypeError:
            job_instances = []
        return job_instances

    def _get_timed_out_jobs(self):
        """Returns timed_out jobs as a list of JobInstances.

        TODO: Explore whether there is any utility in in a
        "from_wire_as_dataframe" utility method on JobInstance, similar to the
        current "from_wire" utility.
        """
        try:
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_timed_out',
                'kwargs': {'dag_id': self.dag_id}
            })
            if rc != ReturnCodes.OK:
                job_instances = []
        except TypeError:
            job_instances = []
        job_instances = [JobInstance.from_wire(ji) for ji in job_instances]
        return job_instances

    def _log_timeout_hostname(self, job_instance_id, hostname):
        msg = {
            'action': 'log_nodename',
            'args': [job_instance_id],
            'kwargs': {'nodename': hostname}
        }
        return self.jsm_req.send_request(msg)

    def _log_mysterious_error(self, job_instance_id, executor_id):
        return self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': ("Job no longer visible in qstat, "
                                         "check qacct or jobmon database for "
                                         "executor_id {} and "
                                         "job_instance_id {}"
                                         .format(executor_id, job_instance_id))
                       }
        })

    def _log_timeout_error(self, job_instance_id):
        return self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Timed out"}
        })

    def _request_permission_to_reconcile(self):
        # sync
        return self.jsm_req.send_request({
            'action': 'log_heartbeat',
            'kwargs': {'dag_id': self.dag_id}
        })
