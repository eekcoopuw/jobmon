import logging
import time
from datetime import datetime
from threading import Event, Thread

from jobmon.client.the_client_config import get_the_client_config
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.job_management.job_factory import JobFactory
from jobmon.client.swarm.job_management.job_instance_factory import \
    JobInstanceFactory
from jobmon.client.swarm.job_management.job_instance_reconciler import \
    JobInstanceReconciler
from jobmon.client.requester import Requester
from jobmon.client.swarm.workflow.executable_task import BoundTask


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 reconciliation_interval=10,
                 job_instantiation_interval=1, interrupt_on_error=True):
        """Manages all the list of jobs that are running, done or errored
        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
            SequentialExecutor, DummyExecutor or SGEExecutor
            start_daemons (bool, default False): whether or not to start the
            JobInstanceFactory and JobReconciler as daemonized threads
            reconciliation_interval (int, default 10 ): number of seconds to
            wait between reconciliation attemps
            job_instantiation_interval (int, default 1): number of seconds to
            wait between instantiating newly ready jobs
            interrupt_on_error (bool, default True): whether or not to
            interrupt the thread if there's an error
        """
        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_inst_factory = JobInstanceFactory(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)

        self.jqs_req = Requester(get_the_client_config(), 'jqs')

        self.bound_tasks = {}  # {job_id: BoundTask}
        self.hash_job_map = {}  # {job_hash: job}
        self.job_hash_map = {}  # {job: job_hash}

        self.all_done = set()
        self.all_error = set()
        self.last_sync = None
        self._sync()

        self.reconciliation_interval = reconciliation_interval
        self.job_instantiation_interval = job_instantiation_interval
        if start_daemons:
            self._start_job_instance_manager()

    @property
    def active_jobs(self):
        """List of tasks that are listed as Registered, Done or Error_Fatal"""
        return [task for job_id, task in self.bound_tasks.items()
                if task.status not in [JobStatus.REGISTERED,
                                       JobStatus.DONE,
                                       JobStatus.ERROR_FATAL]]

    def bind_task(self, task):
        """Bind a task to the database, making it a job
        Args:
            task (obj): obj of a type inherited from ExecutableTask
        """
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
        """Query the database for the status of all jobs"""
        if self.last_sync:
            rc, response = self.jqs_req.send_request(
                app_route='/dag/{}/job'.format(self.dag_id),
                message={'last_sync': str(self.last_sync)},
                request_type='get')
        else:
            rc, response = self.jqs_req.send_request(
                app_route='/dag/{}/job'.format(self.dag_id),
                message={},
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
        self.last_sync = datetime.utcnow()
        return jobs

    def parse_done_and_errors(self, jobs):
        """Separate out the done jobs from the errored ones
        Args:
            jobs(list): list of objects of type models.Job
        """
        completed_tasks = []
        completed_jobs = []
        failed_tasks = []
        failed_jobs = []
        for job in jobs:
            task = self.bound_tasks[job.job_id]
            if task.status == JobStatus.DONE:
                completed_tasks += [task]
                completed_jobs += [job]
            elif (task.status == JobStatus.ERROR_FATAL):
                failed_tasks += [task]
                failed_jobs += [job]
            else:
                continue
        self.all_done.update(set(completed_jobs))
        self.all_error -= set(completed_jobs)
        self.all_error.update(set(failed_jobs))
        return completed_tasks, failed_tasks

    def _sync(self):
        """Get all jobs from the database and parse the done and errored"""
        jobs = self.get_job_statuses()
        self.parse_done_and_errors(jobs)

    def block_until_any_done_or_error(self, timeout=36000, poll_interval=10):
        """Block code execution until a job is done or errored"""
        time_since_last_update = 0
        while True:
            if time_since_last_update > timeout:
                return None
            jobs = self.get_job_statuses()
            completed, failed = self.parse_done_and_errors(jobs)
            if completed or failed:
                return completed, failed
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def block_until_no_instances(self, timeout=36000, poll_interval=10,
                                 raise_on_any_error=True):
        """Block code execution until there are no jobs left"""
        logger.info("Blocking, poll interval = {}".format(poll_interval))

        time_since_last_update = 0
        while True:
            if time_since_last_update > timeout:
                return None
            self._sync()

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

        return list(self.all_done), list(self.all_error)

    def _create_job(self, *args, **kwargs):
        """Create a job by passing the job args/kwargs through to the
        JobFactory
        """
        job = self.job_factory.create_job(*args, **kwargs)
        self.hash_job_map[job.job_hash] = job
        self.job_hash_map[job] = job.job_hash
        return job

    def queue_job(self, job):
        """Queue a job by passing the job's id to the JobFactory"""
        self.job_factory.queue_job(job.job_id)
        task = self.bound_tasks[job.job_id]
        task.status = JobStatus.QUEUED_FOR_INSTANTIATION

    def queue_task(self, task):
        """Add a task's hash to the hash_job_map"""
        job = self.hash_job_map[task.hash]
        self.queue_job(job)

    def reset_jobs(self):
        """Reset jobs by passing through to the JobFactory"""
        self.job_factory.reset_jobs()

    def add_job_attribute(self, job, attribute_type, value):
        """Add a job_attribute to a job by passing thorugh to the JobFactory"""
        self.job_factory.add_job_attribute(job.job_id, attribute_type, value)

    def status_from_hash(self, job_hash):
        """Get the status of a job from its hash"""
        job = self.hash_job_map[job_hash]
        return self.status_from_job(job)

    def status_from_job(self, job):
        """Get the status of a job by its ID"""
        return self.bound_tasks[job.job_id].status

    def status_from_task(self, task):
        """Get the status of a task from its hash"""
        return self.status_from_hash(task.hash)

    def bound_task_from_task(self, task):
        """Get a BoundTask from a regular Task"""
        job = self.hash_job_map[task.hash]
        return self.bound_tasks[job.job_id]

    def _start_job_instance_manager(self):
        """Start the JobInstanceFactory and JobReconciler in separate
        threads
        """
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

    def disconnect(self):
        self._stop_event.set()
