import logging
from time import sleep

import pandas as pd

from jobmon import sge
from jobmon.config import config
from jobmon.models import JobInstance
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)

    def reconcile_periodically(self, poll_interval=10):
        logger.info("Reconciling jobs against 'qstat' at {}s "
                    "intervals".format(poll_interval))
        while True:
            logging.debug("Reconciling at interval {}s".format(poll_interval))
            self.reconcile()
            self.terminate_timed_out_jobs()
            sleep(poll_interval)

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
        to_df = pd.DataFrame.from_dict(self._get_timed_out_jobs())
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
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_submitted_or_running',
                'kwargs': {'dag_id': self.dag_id}
            })
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
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_timed_out',
                'kwargs': {'dag_id': self.dag_id}
            })
        except TypeError:
            job_instances = []
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
                                         "sge executor_id {} and "
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
            'action': 'log_hearbeat',
            'kwargs': {'dag_id': self.dag_id}
        })
