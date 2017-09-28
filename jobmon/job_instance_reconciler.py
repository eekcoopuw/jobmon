import logging
from time import sleep

from jobmon import config, sge
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceReconciler(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)

    def reconcile_periodically(self, poll_interval=1):
        logger.info("Reconciling jobs against 'qstat' at {}s "
                    "intervals".format(poll_interval))
        while True:
            logging.debug("Reconciling at interval {}s".format(poll_interval))
            self.reconcile()
            sleep(poll_interval)

    def reconcile(self):
        presumed = self._get_presumed_instantiated_or_running()
        self._request_permission_to_reconcile()
        actual = self._get_actual_instantiated_or_running()

        # This is kludgy... Re-visit the data structure used for communicating
        # executor IDs back from the JobQueryServer
        missing_exec_ids = set([v for k,v in presumed.items()]) - set(actual)
        missing_job_instance_ids = [k for k, v in presumed.items()
                                    if v in missing_exec_ids]
        for instance_id in missing_job_instance_ids:
            self._log_error(instance_id)
        return missing_job_instance_ids

    def _get_actual_instantiated_or_running(self):
        # TODO: If we formalize the "Executor" concept as more than a
        # command-runner, this should probably be an option method
        # provided by any given Executor
        # ...
        # For now, just qstat
        qstat_out = sge.qstat()
        job_ids = list(qstat_out.job_id)
        job_ids = [int(jid) for jid in job_ids]
        return job_ids

    def _get_presumed_instantiated_or_running(self):
        rc, executor_ids = self.jqs_req.send_request({
            'action': 'get_active_executor_ids',
            'kwargs': {'dag_id': self.dag_id}
        })
        try:
            # Convert keys back to integer ids, for convenience
            executor_ids = {int(k): v for k, v in executor_ids.items()}
        except TypeError:
            # Ignore if there are no active job instances
            pass
        return executor_ids

    def _log_error(self, job_instance_id):
        return self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Job has mysteriously disappeared"}
        })

    def _request_permission_to_reconcile(self):
        # sync
        return self.jsm_req.send_request({'action': 'alive'})

    def _terminate_timed_out_jobs(self):
        pass
