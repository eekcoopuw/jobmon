from time import sleep

from jobmon import config
from jobmon import models
from jobmon.database import session_scope
from jobmon.requester import Requester


class JobInstanceReconciler(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = Requester(config.jm_conn_obj)

    def reconcile_periodically(self, poll_interval=1):
        while True:
            print("Reconciling stuff")
            self.reconcile()
            sleep(poll_interval)

    def reconcile(self):
        with session_scope() as session:
            presumed = self._get_presumed_instantiated_or_running(session)
        self._request_permission_to_reconcile()
        actual = self._get_actual_instantiated_or_running()

        presumed = []
        actual = []
        missing = set(presumed) - set(actual)
        pass

    def _get_actual_instantiated_or_running(self):
        # qstat
        pass

    def _get_presumed_instantiated_or_running(self, session):
        instantiated_jobs = session.query(models.Job).filter_by(
            status=models.JobStatus.INSTANTIATED).all()
        running_jobs = session.query(models.Job).filter_by(
            status=models.JobStatus.INSTANTIATED).all()
        return instantiated_jobs + running_jobs

    def _log_error(self, job_instance_id):
        self.requester.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': job_instance_id,
                       'error_message': "Job has mysteriously disappeared"}
        })

    def _request_permission_to_reconcile(self):
        # sync
        pass

    def _terminate_timed_out_jobs(self):
        pass
