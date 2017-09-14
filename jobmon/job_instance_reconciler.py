import logging
from time import sleep

from jobmon import config
from jobmon.models import Job
from jobmon.database import session_scope
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
        with session_scope() as session:
            presumed = self._get_presumed_instantiated_or_running()
        self._request_permission_to_reconcile()
        actual = self._get_actual_instantiated_or_running()

        presumed = []
        actual = []
        missing = set(presumed) - set(actual)
        pass

    def _get_actual_instantiated_or_running(self):
        # qstat
        pass

    def _get_presumed_instantiated_or_running(self):
        rc, jobs = self.jsm_req.send_request({
            'action': 'get_active',
            'kwargs': {'adg_id': self.dag_id}
        })
        jobs = [Job.from_wire(j) for j in jobs]
        return jobs

    def _log_error(self, job_instance_id):
        self.jsm_req.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Job has mysteriously disappeared"}
        })

    def _request_permission_to_reconcile(self):
        # sync
        pass

    def _terminate_timed_out_jobs(self):
        pass
