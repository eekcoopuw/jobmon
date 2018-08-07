import logging
import time
from threading import Event, Thread
from queue import Queue, Empty

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.models import Job, JobStatus
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_instance_reconciler import JobInstanceReconciler
from jobmon.requester import Requester
from jobmon.subscriber import Subscriber
from jobmon.workflow.executable_task import BoundTask


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
                 job_instantiation_interval=1, interrupt_on_error=True):

        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_inst_factory = JobInstanceFactory(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)

        self.jqs_req = Requester(config.jqs_rep_conn)

        self.db_sync_interval = None
        self.done_queue = Queue()
        self.error_queue = Queue()
        self.update_queue = Queue()
        self.disconnect_queue = Queue()

        self.bound_tasks = {}  # {job_id: BoundTask}
        self.hash_job_map = {}  # {job_hash: job}
        self.job_hash_map = {}  # {job: job_hash}
        self.all_done = set()
        self.all_error = set()
        with session_scope() as session:
            self._sync(session)

        self.reconciliation_interval = reconciliation_interval
        self.job_instantiation_interval = job_instantiation_interval
        if start_daemons:
            self._start_job_status_listener()
            self._start_job_instance_manager()

    @property
    def active_jobs(self):
        return [task for job_id, task in self.bound_tasks.items()
                if task.status not in [JobStatus.REGISTERED,
                                       JobStatus.DONE,
                                       JobStatus.ERROR_FATAL]]

    def bind_task(self, task):
        if task.hash in self.hash_job_map:
            job = self.hash_job_map[task.hash]
        else:
            job = self._create_job(
                jobname=task.name,
                job_hash=task.hash,
                command=task.command,
                tag=task.tag,
                slots=task.slots,
                mem_free=task.mem_free,
                max_attempts=task.max_attempts,
                max_runtime=task.max_runtime,
                context_args=task.context_args,
            )

        #adding the attributes to the job now that there is a job_id
        for attribute in task.job_attributes:
            self.job_factory.add_job_attribute(job.job_id, attribute, task.job_attributes[attribute])

        bound_task = BoundTask(task=task, job=job, job_list_manager=self)
        self.bound_tasks[job.job_id] = bound_task
        return bound_task

    def block_until_any_done_or_error(self, timeout=None):
        """Returns bound tasks that have either completed or failed since last
        called, or blocks until an update is received.

        Be careful. Job statuses are cached in two places: in the update_queue,
        and in the bound_tasks list. Be sure to update in both.

        Args:
            timeout (int, optional): Maximum time to block. If None (default),
                block forever.

        Returns:
            Two lists of Tasks:  (CompletedTasks[], FailedTasks[])
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

        completed = []
        completed_ids = []
        failed = []
        failed_ids = []
        for job_id, status in updates:
            task = self.bound_tasks[job_id]
            task.status = status
            if task.status == JobStatus.DONE:
                completed += [task]
                completed_ids += [task.job_id]
            elif task.status == JobStatus.ERROR_FATAL:
                failed += [task]
                failed_ids += [task.job_id]
            else:
                raise ValueError("Job returned that is neither done nor "
                                 "error_fatal: jid: {}, status {}"
                                 .format(job_id, status))
        self.all_done.update(set(completed_ids))
        self.all_error -= set(completed_ids)
        self.all_error.update(set(failed_ids))
        return completed, failed

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

    def _create_job(self, *args, **kwargs):
        job = self.job_factory.create_job(*args, **kwargs)
        self.hash_job_map[job.job_hash] = job
        self.job_hash_map[job] = job.job_hash
        return job

    def disconnect(self):
        self.job_factory.requester.disconnect()
        self.job_inst_factory.jqs_req.disconnect()
        self.job_inst_factory.jsm_req.disconnect()
        self.job_inst_reconciler.jqs_req.disconnect()
        self.job_inst_reconciler.jsm_req.disconnect()
        self.jqs_req.disconnect()
        self.disconnect_queue.put('stop')
        self._stop_event.set()

    def get_new_done(self):
        new_done = []
        while not self.done_queue.empty():
            done_id = self.done_queue.get()
            new_done.append(done_id)
            self.bound_tasks[done_id].status = JobStatus.DONE
        self.all_done.update(new_done)
        self.all_error -= set(new_done)
        self._sync_at_interval()
        return new_done

    def get_new_errors(self):
        new_errors = []
        while not self.error_queue.empty():
            error_id = self.error_queue.get()
            new_errors.append(error_id)
            self.bound_tasks[error_id].status = JobStatus.ERROR_FATAL
        self.all_error.update(new_errors)
        self._sync_at_interval()
        return new_errors

    def queue_job(self, job):
        self.job_factory.queue_job(job.job_id)
        task = self.bound_tasks[job.job_id]
        task.status = JobStatus.QUEUED_FOR_INSTANTIATION

    def queue_task(self, task):
        job = self.hash_job_map[task.hash]
        self.queue_job(job)

    def reset_jobs(self):
        self.job_factory.reset_jobs()
        with session_scope() as session:
            self._sync(session)

    def status_from_hash(self, job_hash):
        job = self.hash_job_map[job_hash]
        return self.status_from_job(job)

    def status_from_job(self, job):
        return self.bound_tasks[job.job_id].status

    def status_from_task(self, task):
        return self.status_from_hash(task.hash)

    def bound_task_from_task(self, task):
        job = self.hash_job_map[task.hash]
        return self.bound_tasks[job.job_id]

    def _sync(self, session):
        rc, jobs = self.jqs_req.send_request({
            'action': 'get_all_jobs',
            'kwargs': {'dag_id': self.dag_id}
        })
        jobs = [Job.from_wire(j) for j in jobs]
        for job in jobs:
            if job.job_id in self.bound_tasks:
                self.bound_tasks[job.job_id].status = job.status
            else:
                self.bound_tasks[job.job_id] = BoundTask(
                    task=None, job=job, job_list_manager=self)
            self.hash_job_map[job.job_hash] = job
            self.job_hash_map[job] = job.job_hash
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

    def add_job_attributes(self, job, attribute_type, value):
        self.job_factory.add_job_attribute(job.job_id, attribute_type, value)
