import logging
import time
from typing import Dict

from jobmon.client.job_management.job_factory import JobFactory
from jobmon.client.workflow.executable_task import BoundTask, ExecutableTask
from jobmon.client.job_management.swarm_job import SwarmJob
from jobmon.models.job_status import JobStatus
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.requester import shared_requester


logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id):
        """Manages all the list of jobs that are running, done or errored

        Args:
            dag_id (int): the id for the dag to run
        """
        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self.requester = shared_requester

        self.bound_tasks: Dict[int, BoundTask] = {}  # {job_id: BoundTask}
        self.hash_job_map: Dict[int, SwarmJob] = {}  # {job_hash: simpleJob}
        self.job_hash_map: Dict[int, int] = {}  # {job_id: job_hash}

        self.all_done: set = set()
        self.all_error: set = set()
        self.last_sync = None
        self._sync()

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

        if task.hash in self.hash_job_map:
            logger.info("Job already bound and has a hash, retrieving from db "
                        "and making sure updated parameters are bound")
            job = self.hash_job_map[task.hash]

            # update the job's params in case they were changed before resuming
            self._add_parameters(job.job_id, task.executor_parameters,
                                 ExecutorParameterSetType.ORIGINAL)
            task.executor_parameters.validate()
            self._add_parameters(job.job_id, task.executor_parameters,
                                 ExecutorParameterSetType.VALIDATED)
            self._update_job(job.job_id, task.tag, task.max_attempts)
        else:
            job = self._create_job(
                jobname=task.name,
                job_hash=task.hash,
                command=task.command,
                tag=task.tag,
                num_cores=task.executor_parameters.num_cores,
                m_mem_free=task.executor_parameters.m_mem_free,
                max_attempts=task.max_attempts,
                max_runtime_seconds=(
                    task.executor_parameters.max_runtime_seconds),
                context_args=task.executor_parameters.context_args,
                queue=task.executor_parameters.queue,
                j_resource=task.executor_parameters.j_resource,
                resource_scales=task.executor_parameters.resource_scales,
                hard_limits=task.executor_parameters.hard_limits
            )
            task.executor_parameters.validate()
            self._add_parameters(job.job_id, task.executor_parameters,
                                 ExecutorParameterSetType.VALIDATED)

        # adding the attributes to the job now that there is a job_id
        for attribute in task.job_attributes:
            self.job_factory.add_job_attribute(
                job.job_id, attribute, task.job_attributes[attribute])

        bound_task = BoundTask(task=task, job=job, job_list_manager=self)
        self.bound_tasks[job.job_id] = bound_task
        return bound_task

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

    def disconnect(self):
        pass

    def connect(self):
        pass
