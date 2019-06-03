import logging
import time
from threading import Event, Thread

from jobmon.client import shared_requester
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.job_management.job_factory import JobFactory
from jobmon.client.swarm.job_management.job_instance_factory import \
    JobInstanceFactory
from jobmon.client.swarm.job_management.job_instance_reconciler import \
    JobInstanceReconciler
from jobmon.client.swarm.workflow.executable_task import (BoundTask,
                                                          ExecutableTask)
from jobmon.client.swarm.job_management.swarm_job import SwarmJob


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 job_instantiation_interval=3,
                 interrupt_on_error=True, n_queued_jobs=1000,
                 requester=shared_requester):
        """Manages all the list of jobs that are running, done or errored
        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
                SequentialExecutor, DummyExecutor or SGEExecutor
            start_daemons (bool, default False): whether or not to start the
                JobInstanceFactory and JobReconciler as daemonized threads
            job_instantiation_interval (int, default 3): number of seconds to
                wait between instantiating newly ready jobs
            interrupt_on_error (bool, default True): whether or not to
                interrupt the thread if there's an error
            n_queued_jobs (int): number of queued jobs that should be returned
                to be instantiated
        """
        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_instance_factory = JobInstanceFactory(
            dag_id, executor, interrupt_on_error, n_queued_jobs,
            stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id, executor, interrupt_on_error, stop_event=self._stop_event)

        self.requester = shared_requester

        self.bound_tasks = {}  # {job_id: BoundTask}
        self.hash_job_map = {}  # {job_hash: simpleJob}
        self.job_hash_map = {}  # {simpleJob: job_hash}

        self.all_done = set()
        self.all_error = set()
        self.last_sync = None
        self._sync()

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

    def bind_task(self, task: ExecutableTask):
        """Bind a task to the database, making it a job
        Args:
            task (obj): obj of a type inherited from ExecutableTask
        """
        # bind original parameters and validated parameters to the db
        params = task.executor_param_objects['validated']

        if task.hash in self.hash_job_map:
            job = self.hash_job_map[task.hash]
        else:
            job = self._create_job(
                jobname=task.name,
                job_hash=task.hash,
                command=task.command,
                tag=task.tag,
                num_cores=params.num_cores,
                m_mem_free=params.m_mem_free,
                max_attempts=task.max_attempts,
                max_runtime_seconds=params.max_runtime_seconds,
                context_args=params.context_args,
                queue=params.queue,
                j_resource=params.j_resource
            )

        # adding the attributes to the job now that there is a job_id
        for attribute in task.job_attributes:
            self.job_factory.add_job_attribute(
                job.job_id, attribute, task.job_attributes[attribute])

        bound_task = BoundTask(task=task, job=job, job_list_manager=self)
        self.bound_tasks[job.job_id] = bound_task
        return bound_task

    def get_job_statuses(self):
        """Query the database for the status of all jobs"""
        if self.last_sync:
            rc, response = self.requester.send_request(
                app_route='/dag/{}/job_status'.format(self.dag_id),
                message={'last_sync': str(self.last_sync)},
                request_type='get')
        else:
            rc, response = self.requester.send_request(
                app_route='/dag/{}/job_status'.format(self.dag_id),
                message={},
                request_type='get')
        logger.debug("JLM::get_job_statuses(): rc is {} and response is {}".
                     format(rc, response))
        utcnow = response['time']
        self.last_sync = utcnow

        jobs = [SwarmJob.from_wire(job) for job in response['job_dcts']]
        for job in jobs:
            if job.job_id in self.bound_tasks.keys():
                self.bound_tasks[job.job_id].status = job.status
            else:
                # This should really only happen the first time
                # _sync() is called when resuming a WF/DAG. This
                # BoundTask really only serves as a
                # dummy/placeholder for status until the Task can
                # actually be bound. To put it another way, if not
                # this_bound_task.is_bound: ONLY USE IT TO DETERMINE
                # IF THE TASK WAS PREVIOUSLY DONE. This branch may
                # be subject to removal altogether if it can be
                # determined that there is a better way to determine
                # previous state in resume cases
                self.bound_tasks[job.job_id] = BoundTask(
                    task=None, job=job, job_list_manager=self)
            self.hash_job_map[job.job_hash] = job
            self.job_hash_map[job.job_id] = job.job_hash
        return jobs

    def parse_done_and_errors(self, jobs):
        """Separate out the done jobs from the errored ones
        Args:
            jobs(list): list of objects of type models.Job
        """
        completed_tasks = set()
        failed_tasks = set()
        for job in jobs:
            task = self.bound_tasks[job.job_id]
            if task.status == JobStatus.DONE and task not in self.all_done:
                completed_tasks.add(task)
            elif (task.status == JobStatus.ERROR_FATAL and
                  task not in self.all_error and
                  task.is_bound):
                # if the task is NOT yet bound, then we must be
                # resuming the Workflow. In that case, we do not
                # want to account for this task in our current list
                # of failures as it is about to be reset and
                # retried... i.e. move on to the else: continue
                failed_tasks.add(task)
            else:
                continue
        self.all_done.update(completed_tasks)
        self.all_error -= completed_tasks
        self.all_error.update(failed_tasks)
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
                raise RuntimeError("Not all tasks completed within the given "
                                   "workflow timeout length ({} seconds). "
                                   "Submitted tasks will still run, but the "
                                   "workflow will need to be restarted."
                                   .format(timeout))
            jobs = self.get_job_statuses()
            completed, failed = self.parse_done_and_errors(jobs)
            if completed or failed:
                return completed, failed
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def _create_job(self, *args, **kwargs):
        """Create a job by passing the job args/kwargs through to the
        JobFactory
        """
        job = self.job_factory.create_job(*args, **kwargs)
        self.hash_job_map[job.job_hash] = job
        self.job_hash_map[job.job_id] = job.job_hash
        return job

    def queue_job(self, variable):
        """Queue a job by passing the job's id to the JobFactory"""
        job_id = variable
        if str(type(variable)) != "<class 'int'>":
            # what we really need here is a job_id, so take job_id as variable
            # for the optimized query it seems some clients are using the
            # function, so support the old way thus, I have to take advantage
            # of the typeless feature of python, which is a bad practise
            job_id = variable.job_id
        self.job_factory.queue_job(job_id)
        task = self.bound_tasks[job_id]
        task.status = JobStatus.QUEUED_FOR_INSTANTIATION

    def queue_task(self, task):
        """Add a task's hash to the hash_job_map"""
        job_id = self.hash_job_map[task.hash].job_id
        self.queue_job(job_id)

    def reset_jobs(self):
        """Reset jobs by passing through to the JobFactory"""
        self.job_factory.reset_jobs()
        self._sync()

    def add_job_attribute(self, job_id, attribute_type, value):
        """Add a job_attribute to a job by passing thorugh to the JobFactory"""
        self.job_factory.add_job_attribute(job_id, attribute_type, value)

    def status_from_hash(self, job_hash):
        """Get the status of a job from its hash"""
        job_id = self.hash_job_map[job_hash].job_id
        return self.status_from_job(job_id)

    def status_from_job(self, job_id):
        """Get the status of a job by its ID"""
        return self.bound_tasks[job_id].status

    def status_from_task(self, task):
        """Get the status of a task from its hash"""
        return self.status_from_hash(task.hash)

    def bound_task_from_task(self, task):
        """Get a BoundTask from a regular Task"""
        job_id = self.hash_job_map[task.hash].job_id
        return self.bound_tasks[job_id]

    def _start_job_instance_manager(self):
        """Start the JobInstanceFactory and JobReconciler in separate
        threads
        """
        self.jif_proc = Thread(
            target=self.job_instance_factory.instantiate_queued_jobs_periodically,
            args=(self.job_instantiation_interval,))
        self.jif_proc.daemon = True
        self.jif_proc.start()

        self.jir_proc = Thread(
            target=self.job_inst_reconciler.reconcile_periodically)
        self.jir_proc.daemon = True
        self.jir_proc.start()

    def disconnect(self):
        self._stop_event.set()
