import logging
from time import sleep

from jobmon import config, sge
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
        presumed = self._get_presumed_instantiated_or_running()
        self._request_permission_to_reconcile()
        actual = self._get_actual_instantiated_or_running()

        # This is kludgy... Re-visit the data structure used for communicating
        # executor IDs back from the JobQueryServer
        missing_job_instance_ids = []
        for job_instance in presumed:
            if job_instance.executor_id:
                if job_instance.executor_id not in actual:
                    job_instance_id = job_instance.job_instance_id
                    self._log_mysterious_error(job_instance_id)
                    missing_job_instance_ids.append(job_instance_id)
        return missing_job_instance_ids

    def terminate_timed_out_jobs(self):
        job_instances = self._get_timed_out_jobs()
        if job_instances:
            sge.qdel([ji.executor_id for ji in job_instances])
        for ji in job_instances:
            self._log_timeout_error(ji.job_instance_id)

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
        try:
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_active',
                'kwargs': {'dag_id': self.dag_id}
            })
            job_instances = [JobInstance.from_wire(j) for j in job_instances]
        except TypeError:
            job_instances = []
        return job_instances

    def _get_timed_out_jobs(self):
        try:
            rc, job_instances = self.jqs_req.send_request({
                'action': 'get_timed_out',
                'kwargs': {'dag_id': self.dag_id}
            })
            job_instances = [JobInstance.from_wire(j) for j in job_instances]
        except TypeError:
            job_instances = []
        return job_instances

    def _log_mysterious_error(self, job_instance_id):
        return self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Job has mysteriously disappeared"}
        })

    def _log_timeout_error(self, job_instance_id):
        return self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Timed out"}
        })

    def _request_permission_to_reconcile(self):
        # sync
        return self.jsm_req.send_request({'action': 'alive'})
