import logging
import time
from threading import Thread
from datetime import datetime

from jobmon.config import config
from jobmon.models import Job, JobStatus
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_instance_reconciler import JobInstanceReconciler
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 reconciliation_interval=10,
                 job_instantiation_interval=1, interrupt_on_error=True):

        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self.job_inst_factory = JobInstanceFactory(dag_id, executor,
                                                   interrupt_on_error)
        self.job_inst_reconciler = JobInstanceReconciler(dag_id,
                                                         interrupt_on_error)

        self.jqs_req = Requester(config.jqs_port)

        self.job_statuses = {}  # {job_id: status_id}
        self.all_done = set()
        self.all_error = set()

        self.reconciliation_interval = reconciliation_interval
        self.job_instantiation_interval = job_instantiation_interval
        if start_daemons:
            self._start_job_instance_manager()

    @classmethod
    def from_new_dag(cls, executor=None, start_daemons=False):

        # TODO: This should really be the work of the DAG manager itself,
        # but for the sake of expediting early development work, allow the
        # JobListManager to obtain it's own dag_id
        from jobmon.config import config
        from jobmon.requester import Requester

        req = Requester(config.jsm_port)
        rc, response = req.send_request(
            app_route='/add_task_dag',
            message={'name': 'test dag', 'user': 'test user',
                     'dag_hash': 'hash', 'created_date': datetime.utcnow()},
            request_type='post')
        return cls(response['dag_id'], executor=executor,
                   start_daemons=start_daemons)

    @property
    def active_jobs(self):
        return [job_id for job_id, job_status in self.job_statuses.items()
                if job_status not in [JobStatus.REGISTERED,
                                      JobStatus.DONE,
                                      JobStatus.ERROR_FATAL]]

    def get_job_statuses(self):
        rc, response = self.jqs_req.send_request(
            app_route='/get_jobs',
            message={'dag_id': self.dag_id},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
        return jobs

    def parse_done_and_errors(self, jobs):
        for job in jobs:
            if job.job_status == JobStatus.DONE:
                self.all_done.add(job.jd)
            elif job.job_status == JobStatus.ERROR_FATAL:
                self.all_error.add(job.id)
            self.job_statuses[job.id] = job.job_status
        return self.all_done.union(self.all_error)

    def block_until_any_done_or_error(self, timeout=36000, poll_interval=10):
        time_since_last_update = 0
        while True:
            if time_since_last_update > timeout:
                return None
            jobs = self.get_job_statuses()
            updates = self.parse_done_and_errors(jobs)
            if updates:
                return updates
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def block_until_no_instances(self, timeout=36000, poll_interval=10,
                                 raise_on_any_error=True):
        logger.info("Blocking, poll interval = {}".format(poll_interval))

        time_since_last_update = 0
        while True:
            if time_since_last_update > timeout:
                return None
            jobs = self.get_job_statuses()
            self.parse_done_and_errors(jobs)

            if len(self.active_jobs) == 0:
                break

            if raise_on_any_error:
                if len(self.all_error) > 0:
                    raise RuntimeError("1 or more jobs encountered a "
                                       "fatal error")
            logger.debug("{} active jobs. Waiting {} seconds...".format(
                len(self.active_jobs), poll_interval))
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

        return self.all_done, self.all_errors

    def create_job(self, *args, **kwargs):
        job_id = self.job_factory.create_job(*args, **kwargs)
        self.job_statuses[job_id] = JobStatus.REGISTERED
        return job_id

    def queue_job(self, job_id):
        self.job_factory.queue_job(job_id)
        self.job_statuses[job_id] = JobStatus.QUEUED_FOR_INSTANTIATION

    def reset_jobs(self):
        self.job_factory.reset_jobs()

    def _start_job_instance_manager(self):
        self.jif_proc = Thread(
            target=self.job_inst_factory.instantiate_queued_jobs_periodically,
            args=(self.job_instantiation_interval,))
        self.jif_proc.daemon = True
        self.jif_proc.start()

        self.jir_proc = Thread(
            target=self.job_inst_reconciler.reconcile_periodically,
            args=(self.reconciliation_interval,))
        self.jir_proc.daemon = True
        self.jir_proc.start()
