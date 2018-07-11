import logging
import time
from threading import Event, Thread
from datetime import datetime

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.models import Job, JobStatus
from jobmon.job_factory import JobFactory
from jobmon.job_instance_factory import JobInstanceFactory
from jobmon.job_instance_reconciler import JobInstanceReconciler
from jobmon.requester import Requester
from jobmon.workflow.executable_task import BoundTask


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 reconciliation_interval=10,
                 job_instantiation_interval=1, interrupt_on_error=True):

        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_inst_factory = JobInstanceFactory(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id, interrupt_on_error, stop_event=self._stop_event)

        self.jqs_req = Requester(config.jqs_port)

        self.bound_tasks = {}  # {job_id: BoundTask}
        self.hash_job_map = {}  # {job_hash: job}
        self.job_hash_map = {}  # {job: job_hash}

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
        bound_task = BoundTask(task=task, job=job, job_list_manager=self)
        self.bound_tasks[job.job_id] = bound_task
        return bound_task

    def get_job_statuses(self):
        rc, response = self.jqs_req.send_request(
            app_route='/get_jobs',
            message={'dag_id': self.dag_id},
            request_type='get')
        jobs = [Job.from_wire(j) for j in response['job_dcts']]
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
        return jobs

    def parse_done_and_errors(self, jobs):
        completed = []
        completed_ids = []
        failed = []
        failed_ids = []
        for task in jobs:
            if task.status == JobStatus.DONE:
                completed += [task]
                completed_ids += [task.job_id]
            elif task.status == JobStatus.ERROR_FATAL:
                failed += [task]
                failed_ids += [task.job_id]
            else:
                raise ValueError("Job returned that is neither done nor "
                                 "error_fatal: jid: {}, status {}"
                                 .format(task.job_id, task.status))
        self.all_done.update(set(completed_ids))
        self.all_error -= set(completed_ids)
        self.all_error.update(set(failed_ids))
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
            jobs = self.get_job_statuses
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

    def _create_job(self, *args, **kwargs):
        job = self.job_factory.create_job(*args, **kwargs)
        self.hash_job_map[job.job_hash] = job
        self.job_hash_map[job] = job.job_hash
        return job

    def queue_job(self, job):
        self.job_factory.queue_job(job.job_id)
        task = self.bound_tasks[job.job_id]
        task.status = JobStatus.QUEUED_FOR_INSTANTIATION

    def queue_task(self, task):
        job = self.hash_job_map[task.hash]
        self.queue_job(job)

    def reset_jobs(self):
        self.job_factory.reset_jobs()

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
