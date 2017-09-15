import logging
import time
from multiprocessing import Process, Queue

from jobmon.database import session_scope
from jobmon.models import Job, JobStatus
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_instance_reconciler import JobInstanceReconciler
from jobmon.subscriber import Subscriber


logger = logging.getLogger(__name__)


def listen_for_job_statuses(host, port, dag_id, done_queue, error_queue):
    subscriber = Subscriber(host, port, dag_id)
    while True:
        msg = subscriber.receive()
        job_id, job_status = msg
        if job_status == JobStatus.DONE:
            done_queue.put(job_id)
        elif job_status == JobStatus.ERROR_FATAL:
            error_queue.put(job_id)


class JobListManager(object):

    def __init__(self, dag_id, db_sync_interval=None):

        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)
        self.job_inst_factory = JobInstanceFactory(dag_id)
        self.job_inst_reconciler = JobInstanceReconciler(dag_id)
        self.job_states = {}  # {job_id: status_id}

        self.db_sync_interval = None
        self.done_queue = Queue()
        self.error_queue = Queue()

        self.all_done = set()
        self.all_error = set()

        self.last_sync = time.time()

    def block_until_no_instances(self, poll_interval=10,
                                 raise_on_any_error=True):
        logger.info("Blocking, poll interval = {}".format(poll_interval))

        while True:
            with session_scope() as session:
                active_jobs = self._get_instantiated_not_done_not_fatal(
                    session)
            if len(active_jobs) == 0:
                break

            if raise_on_any_error:
                self.get_new_errors()
                if len(self.all_error) > 0:
                    break

            logger.debug("{} active jobs. Waiting {} seconds...".format(
                len(active_jobs), poll_interval))
            time.sleep(poll_interval)
            self._sync_at_interval()

    def create_job(self, runfile, jobname, parameters=None):
        return self.job_factory.create_job(runfile, jobname, parameters)

    def get_new_done(self):
        new_done = []
        while not self.done_queue.empty():
            new_done.append(self.done_queue.get())
        self.all_done.update(new_done)
        self._sync_at_interval()
        return new_done

    def get_new_errors(self):
        new_errors = []
        while not self.error_queue.empty():
            new_errors.append(self.error_queue.get())
        self.all_errors.update(new_errors)
        self._sync_at_interval()
        return new_errors

    def queue_job(self, job_id):
        self.job_factory(job_id)

    def _sync(self):
        self._sync_done()
        self._sync_fatal()
        self.last_sync = time.time()

    def _sync_at_interval(self):
        if self.db_sync_interval:
            if (time.time() - self.last_sync) > self.db_sync_interval:
                self._sync()

    def _sync_done(self, session):
        jobs = session.query(Job).filter(
            Job.status == JobStatus.DONE,
            Job.dag_id == self.dag_id).all()
        self.all_done = set([job.job_id for job in jobs])
        return jobs

    def _sync_fatal(self, session):
        jobs = session.query(Job).filter(
            Job.status == JobStatus.ERROR_FATAL,
            Job.dag_id == self.dag_id).all()
        self.all_error = set([job.job_id for job in jobs])
        return jobs

    def _get_instantiated_not_done_not_fatal(self, session):
        jobs = session.query(Job).filter(
            Job.status != JobStatus.REGISTERED,
            Job.status != JobStatus.DONE,
            Job.status != JobStatus.ERROR_FATAL,
            Job.dag_id == self.dag_id).all()
        return jobs

    def _start_job_status_listener(self):
        self.jsl_proc = Process(target=listen_for_job_statuses,
                                args=('localhost', 5678, "", self.done_queue,
                                      self.error_queue))
        self.jsl_proc.start()

    def _stop_job_status_listener(self):
        if self.jsl_proc:
            self.jsl_proc.terminate()

    def _start_job_instance_manager(self):
        self.jif_proc = Process(
            target=self.job_inst_factory.instantiate_queued_jobs_periodically)
        self.jif_proc.start()
        self.jir_proc = Process(
            target=self.job_inst_reconciler.reconcile_periodically)
        self.jir_proc.start()

    def _stop_job_instance_manager(self):
        if self.jif_proc:
            self.jim_proc.terminate()
        if self.jir_proc:
            self.jim_proc.terminate()
