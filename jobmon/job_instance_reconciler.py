import logging
import _thread
from time import sleep

import pandas as pd
from zmq.error import ZMQError
from http import HTTPStatus

from jobmon import sge
from jobmon.config import config
from jobmon.models import JobInstance
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):

    def __init__(self, dag_id, interrupt_on_error=True):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_port)
        self.jqs_req = Requester(config.jqs_port)
        self.interrupt_on_error = interrupt_on_error

    def reconcile_periodically(self, poll_interval=10):
        logger.info("Reconciling jobs against 'qstat' at {}s "
                    "intervals".format(poll_interval))
        while True:
            try:
                logging.debug("Reconciling at interval {}s".format(poll_interval))
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
                logger.error(e)
                if self.interrupt_on_error:
                    _thread.interrupt_main()
                else:
                    raise

    def reconcile(self):
        """Identifies jobs that have disappeared from the batch execution
        system (e.g. SGE), and reports their disappearance back to the
        JobStateManager so they can either be retried or flagged as
        fatal errors"""
        presumed = self._get_presumed_submitted_or_running()
        self._request_permission_to_reconcile()
        actual = self._get_actual_submitted_or_running()

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
        to_df = pd.DataFrame.from_dict(to_jobs)
        if len(to_df) == 0:
            return
        sge_jobs = sge.qstat()
        sge_jobs = sge_jobs[~sge_jobs.status.isin(['hqw', 'qw'])]
        to_df = to_df.merge(sge_jobs, left_on='executor_id', right_on='job_id')
        to_df = to_df[to_df.runtime_seconds > to_df.max_runtime]
        if len(to_df) > 0:
            sge.qdel(list(to_df.executor_id))
            for _, row in to_df.iterrows():
                ji_id = row.job_instance_id
                hostname = row.hostname
                logger.debug("Killing timed out JI {}".format(int(ji_id)))
                self._log_timeout_error(int(ji_id))
                self._log_timeout_hostname(int(ji_id), hostname)

    def _get_actual_submitted_or_running(self):
        # TODO: If we formalize the "Executor" concept as more than a
        # command-runner, this should probably be an option method
        # provided by any given Executor
        # ...
        # For now, just qstat
        qstat_out = sge.qstat()
        job_ids = list(qstat_out.job_id)
        job_ids = [int(jid) for jid in job_ids]
        return job_ids

    def _get_presumed_submitted_or_running(self):
        try:
            rc, response = self.jqs_req.send_request(
                app_route='/get_submitted_or_running',
                message={'dag_id': self.dag_id},
                request_type='get')
            job_instances = response['ji_dcts']
            job_instances = [JobInstance.from_wire(j) for j in job_instances]
        except TypeError:
            job_instances = []
        return job_instances

    def _get_timed_out_jobs(self):
        """Returns timed_out jobs as a list of dicts (rather than converting
        them to JobInstance objects). The dicts are more convenient to
        transform into a Pandas.DataFrame downstream, which is joined to qstat
        output to determine whether any jobs should be qdel'd.

        TODO: Explore whether there is any utility in in a
        "from_wire_as_dataframe" utility method on JobInstance, similar to the
        current "from_wire" utility.
        """
        try:
            rc, response = self.jqs_req.send_request(
                app_route='/get_timed_out',
                message={'dag_id': self.dag_id},
                request_type='get')
            job_instances = response['timed_out']
            if rc != HTTPStatus.OK:
                job_instances = []
        except TypeError:
            job_instances = []
        return job_instances

    def _log_timeout_hostname(self, job_instance_id, hostname):
        return self.jsm_req.send_request(
            app_route='/log_nodename',
            message={'job_instance_id': [job_instance_id],
                     'nodename': hostname},
            request_type='post')

    def _log_mysterious_error(self, job_instance_id, executor_id):
        return self.jsm_req.send_request(
            app_route='/log_error',
            message={'job_instance_id': job_instance_id,
                     'error_message': ("Job no longer visible in qstat, "
                                       "check qacct or jobmon database for "
                                       "sge executor_id {} and "
                                       "job_instance_id {}"
                                       .format(executor_id, job_instance_id))},
            request_type='post')

    def _log_timeout_error(self, job_instance_id):
        return self.jsm_req.send_request(
            app_route='/log_error',
            message={'job_instance_id': job_instance_id,
                     'error_message': "Timed out"},
            request_type='post')

    def _request_permission_to_reconcile(self):
        # sync
        return self.jsm_req.send_request(
            app_route='/log_heartbeat',
            message={'dag_id': self.dag_id},
            request_type='post')
