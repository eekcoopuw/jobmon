import logging
import time
from threading import Event, Thread
from typing import Dict

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
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 job_instantiation_interval=3, n_queued_jobs=1000,
                 resource_adjustment: float = 0.5):
        """Manages all the list of jobs that are running, done or errored

        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
                SequentialExecutor, DummyExecutor or SGEExecutor
            start_daemons (bool, default False): whether or not to start the
                JobInstanceFactory and JobReconciler as daemonized threads
            job_instantiation_interval (int, default 3): number of seconds to
                wait between instantiating newly ready jobs
            n_queued_jobs (int): number of queued jobs that should be returned
                to be instantiated
            resource_adjustment: scalar value to adjust resources by when
                a resource error is detected
        """
        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_instance_factory = JobInstanceFactory(
            dag_id=dag_id,
            executor=executor,
            n_queued_jobs=n_queued_jobs,
            resource_adjustment=resource_adjustment,
            stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id=dag_id,
            executor=executor,
            stop_event=self._stop_event)

        self.requester = shared_requester

        self.bound_tasks: Dict[int, BoundTask] = {}  # {job_id: BoundTask}
        self.hash_job_map: Dict[int, SwarmJob] = {}  # {job_hash: simpleJob}
        self.job_hash_map: Dict[int, int] = {}  # {job_id: job_hash}

        self.all_done: set = set()
        self.all_error: set = set()
        self.last_sync = None
        self._sync()

        self.job_instantiation_interval = job_instantiation_interval
        if start_daemons:
            self._start_job_instance_manager()

    @property
    def active_jobs(self):
        """List of tasks that are listed as Adjusting Resources,
        Done or Error_Fatal"""
        return [task for job_id, task in self.bound_tasks.items()
                if task.status not in [JobStatus.ADJUSTING_RESOURCES,
                                       JobStatus.DONE,
                                       JobStatus.ERROR_FATAL]]

    def bind_task(self, task: ExecutableTask):
        """Bind a task to the database, making it a job
        Args:
            task (obj): obj of a type inherited from ExecutableTask
        """
        # bind original parameters and validated parameters to the db

        if task.hash in self.hash_job_map:
            logger.info("Job already bound and has a hash, retrieving from db "
                        "and making sure updated parameters are bound")
            job = self.hash_job_map[task.hash]
        else:
            job = self._create_job(
                jobname=task.name,
                job_hash=task.hash,
                command=task.command,
                tag=task.tag,
                max_attempts=task.max_attempts
            )

        # adding the attributes to the job now that there is a job_id
        for attribute in task.job_attributes:
            self.job_factory.add_job_attribute(
                job.job_id, attribute, task.job_attributes[attribute])

        bound_task = BoundTask(task=task, job=job, job_list_manager=self)
        self.bound_tasks[job.job_id] = bound_task
        return bound_task

    def _bind_parameters(self, job_id, task):
        self._add_parameters(job_id, task.executor_parameters,
                             ExecutorParameterSetType.ORIGINAL)
        task.executor_parameters.validate()
        self._add_parameters(job_id, task.executor_parameters,
                             ExecutorParameterSetType.VALIDATED)
        self._update_job(job_id, task.tag, task.max_attempts)

    def _add_parameters(self, job_id: int, executor_parameters,
                        parameter_set_type=ExecutorParameterSetType.VALIDATED):
        """Add an entry for the validated parameters to the database and
        activate them"""
        msg = {'parameter_set_type': parameter_set_type,
               'max_runtime_seconds': executor_parameters.max_runtime_seconds,
               'context_args': executor_parameters.context_args,
               'queue': executor_parameters.queue,
               'num_cores': executor_parameters.num_cores,
               'm_mem_free': executor_parameters.m_mem_free,
               'j_resource': executor_parameters.j_resource,
               'resource_scales': executor_parameters.resource_scales,
               'hard_limits': executor_parameters.hard_limits}
        self.requester.send_request(
            app_route=f'/job/{job_id}/update_resources',
            message=msg,
            request_type='post')

    def _update_job(self, job_id: int, tag: str, max_attempts: int):
        msg = {'tag': tag, 'max_attempts': max_attempts}
        self.requester.send_request(
            app_route=f'/job/{job_id}/update_job',
            message=msg,
            request_type='post'
        )

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

    def adjust_and_queue(self, task):
        """Add a task's hash to the hash_job_map"""
        job_id = self.hash_job_map[task.hash].job_id
        # at this point it should be in adjusting resources, the problem is
        # that if it is in A state then there will be a race condition with the
        # job instance factory that is polling for jobs in adjusting state
        self.adjust(job_id, task)
        # some sort of differentiation needs to happen
        self.queue_job(job_id)

    def adjust(self, job_id, task):
        """Function from Job Instance Factory that adjusts resources and then
        queues them, this should also incorporate resource binding if they
        have not yet been bound"""
        self._bind_parameters(job_id, task)
        # TODO
        # if job.status == JobStatus.ADJUSTING_RESOURCES:
        #     logger.debug("Job in A state, adjusting resources before queueing")
        #     only_scale = list(job.executor_parameters.resource_scales.keys())
        #     rc, response = self.requester.send_request(
        #         app_route=f'/job/{job.job_id}/most_recent_exec_id',
        #         message={},
        #         request_type='get'
        #     )
        #     if len(response['executor_id']) > 0:
        #         exec_id = response['executor_id'][0]
        #         try:
        #             exit_code, msg = self.executor.get_remote_exit_info(
        #                 exec_id)
        #             if 'exceeded max_runtime' in msg and \
        #                     'max_runtime_seconds' in only_scale:
        #                 only_scale = ['max_runtime_seconds']
        #         except RemoteExitInfoNotAvailable:
        #             logger.debug("Unable to retrieve exit info to "
        #                          "determine the cause of resource error")
        #     job.update_executor_parameter_set(
        #         parameter_set_type=JobStatus.ADJUSTING_RESOURCES,
        #         only_scale=only_scale,
        #         resource_adjustment=self.resource_adjustment)
        #     job.queue_job()

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
            target=(
                self.job_instance_factory.instantiate_queued_jobs_periodically
            ),
            args=(self.job_instantiation_interval,))
        self.jif_proc.daemon = True
        self.jif_proc.start()

        self.jir_proc = Thread(
            target=self.job_inst_reconciler.reconcile_periodically)
        self.jir_proc.daemon = True
        self.jir_proc.start()

    def disconnect(self):
        self._stop_event.set()

    def connect(self):
        self._stop_event = Event()
        self._start_job_instance_manager()
