import logging
import time
from threading import Thread
from queue import Queue, Empty

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.models import Job, JobStatus
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_instance_reconciler import JobInstanceReconciler
from jobmon.requester import Requester
from jobmon.subscriber import Subscriber


logger = logging.getLogger(__name__)


def listen_for_job_statuses(host, port, dag_id, done_queue, error_queue,
                            update_queue, disconnect_queue):
    """ Because we're dealing with threads, this can't be a method on a class
    """
    logger.info("Listening for dag_id={} job status updates from {}:{}".format(
        dag_id, host, port))
    subscriber = Subscriber(host, port, dag_id)

    while True:
        try:
            disconnect_queue.get_nowait()
            break
        except Empty:
            pass
        msg = subscriber.receive()
        job_id, job_status = msg
        if job_status == JobStatus.DONE:
            done_queue.put(job_id)
            update_queue.put((job_id, job_status))
        elif job_status == JobStatus.ERROR_FATAL:
            error_queue.put(job_id)
            update_queue.put((job_id, job_status))


class JobListManager(object):

    def __init__(self, dag_id, executor=None, db_sync_interval=None,
                 start_daemons=False, reconciliation_interval=10,
                 job_instantiation_interval=1):

        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self.job_inst_factory = JobInstanceFactory(dag_id, executor)
        self.job_inst_reconciler = JobInstanceReconciler(dag_id)

        self.jqs_req = Requester(config.jqs_rep_conn)

        self.db_sync_interval = None
        self.done_queue = Queue()
        self.error_queue = Queue()
        self.update_queue = Queue()
        self.disconnect_queue = Queue()

        self.job_statuses = {}  # {job_id: status_id}
        self.all_done = set()
        self.all_error = set()
        with session_scope() as session:
            self._sync(session)

        self.reconciliation_interval = reconciliation_interval
        self.job_instantiation_interval = job_instantiation_interval
        if start_daemons:
            self._start_job_status_listener()
            self._start_job_instance_manager()

    @classmethod
    def from_new_dag(cls, executor=None, start_daemons=False):

        # TODO: This should really be the work of the DAG manager itself,
        # but for the sake of expediting early development work, allow the
        # JobListManager to obtain it's own dag_id
        from jobmon.config import config
        from jobmon.requester import Requester

        req = Requester(config.jm_rep_conn)
        rc, dag_id = req.send_request({
            'action': 'add_task_dag',
            'kwargs': {'name': 'test dag', 'user': 'test user'}
        })
        req.disconnect()
        return cls(dag_id, executor=executor, start_daemons=start_daemons)

    @classmethod
    def in_memory(cls, executor, start_daemons):
        from threading import Thread
        from sqlalchemy.exc import IntegrityError

        from jobmon import database
        from jobmon.config import config
        from jobmon.job_query_server import JobQueryServer
        from jobmon.job_state_manager import JobStateManager

        database.create_job_db()
        try:
            with database.session_scope() as session:
                database.load_default_statuses(session)
        except IntegrityError:
            pass

        jsm = JobStateManager(config.jm_rep_conn.port, config.jm_pub_conn.port)

        jqs = JobQueryServer(config.jqs_rep_conn.port)

        t1 = Thread(target=jsm.listen)
        t1.daemon = True
        t1.start()
        t2 = Thread(target=jqs.listen)
        t2.daemon = True
        t2.start()
        return cls.from_new_dag(executor, start_daemons)

    @property
    def active_jobs(self):
        return [job_id for job_id, job_status in self.job_statuses.items()
                if job_status not in [JobStatus.REGISTERED,
                                      JobStatus.DONE,
                                      JobStatus.ERROR_FATAL]]

    def block_until_any_done_or_error(self, timeout=None):
        """Returns any job updates since last called, or blocks until an update
        is received.

        Be careful. Job statuses are cached in two places: in the update_queue,
        and in the job_status dictionary. Be sure to update in both.

        Args:
            timeout (int, optional): Maximum time to block. If None (default),
                block forever.

        Returns:
            list of (job_id, job_status) tuples
        """
        # TODO: Consider whether the done and error queues should also be
        # cleared by this method... or whether they should continue to be
        # independent of the general update queue
        updates = []
        if not self.update_queue.empty():
            while not self.update_queue.empty():
                updates.append(self.update_queue.get(timeout=timeout))
        else:
            updates = [self.update_queue.get(timeout=timeout)]
        self.job_statuses.update(dict(updates))  # update job_status here
        return updates

    def block_until_no_instances(self, poll_interval=10,
                                 raise_on_any_error=True):
        logger.info("Blocking, poll interval = {}".format(poll_interval))

        done = []
        errors = []
        while True:
            new_done = self.get_new_done()
            done.extend(new_done)

            new_errors = self.get_new_errors()
            errors.extend(new_errors)

            if len(self.active_jobs) == 0:
                break

            if raise_on_any_error:
                if len(self.all_error) > 0:
                    raise RuntimeError("1 or more jobs encountered a "
                                       "fatal error")

            logger.debug("{} active jobs. Waiting {} seconds...".format(
                len(self.active_jobs), poll_interval))
            time.sleep(poll_interval)
            self._sync_at_interval()
        return done, errors

    def create_job(self, *args, **kwargs):
        job_id = self.job_factory.create_job(*args, **kwargs)
        self.job_statuses[job_id] = JobStatus.REGISTERED
        return job_id

    def disconnect(self):
        self.job_factory.requester.disconnect()
        self.job_inst_factory.jqs_req.disconnect()
        self.job_inst_factory.jsm_req.disconnect()
        self.job_inst_reconciler.jqs_req.disconnect()
        self.job_inst_reconciler.jsm_req.disconnect()
        self.jqs_req.disconnect()
        self.disconnect_queue.put('stop')

    def get_new_done(self):
        new_done = []
        while not self.done_queue.empty():
            done_id = self.done_queue.get()
            new_done.append(done_id)
            self.job_statuses[done_id] = JobStatus.DONE
        self.all_done.update(new_done)
        self._sync_at_interval()
        return new_done

    def get_new_errors(self):
        new_errors = []
        while not self.error_queue.empty():
            error_id = self.error_queue.get()
            new_errors.append(error_id)
            self.job_statuses[error_id] = JobStatus.ERROR_FATAL
        self.all_error.update(new_errors)
        self._sync_at_interval()
        return new_errors

    def queue_job(self, job_id):
        self.job_factory.queue_job(job_id)
        self.job_statuses[job_id] = JobStatus.QUEUED_FOR_INSTANTIATION

    def _sync(self, session):
        rc, jobs = self.jqs_req.send_request({
            'action': 'get_all_jobs',
            'kwargs': {'dag_id': self.dag_id}
        })
        jobs = [Job.from_wire(j) for j in jobs]
        for job in jobs:
            self.job_statuses[job.job_id] = job.status
        self.all_done = set([job.job_id for job in jobs
                             if job.status == JobStatus.DONE])
        self.all_error = set([job.job_id for job in jobs if job.status ==
                              JobStatus.ERROR_FATAL])
        self.last_sync = time.time()

    def _sync_at_interval(self):
        if self.db_sync_interval:
            if (time.time() - self.last_sync) > self.db_sync_interval:
                with session_scope() as session:
                    self._sync(session)

    def _start_job_status_listener(self):
        self.jsl_proc = Thread(target=listen_for_job_statuses,
                               args=(config.jm_pub_conn.host,
                                     config.jm_pub_conn.port,
                                     self.dag_id, self.done_queue,
                                     self.error_queue, self.update_queue,
                                     self.disconnect_queue))
        self.jsl_proc.daemon = True
        self.jsl_proc.start()

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
