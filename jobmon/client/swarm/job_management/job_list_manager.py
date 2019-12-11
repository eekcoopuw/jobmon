from functools import partial
import time
from threading import Event, Thread
from typing import Dict
from http import HTTPStatus as StatusCodes

from jobmon.client import shared_requester
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.job_management.job_factory import JobFactory
from jobmon.client.swarm.job_management.job_instance_factory import \
    JobInstanceFactory
from jobmon.client.swarm.job_management.job_instance_reconciler import \
    JobInstanceReconciler
from jobmon.client.swarm.workflow.executable_task import (BoundTask,
                                                          ExecutableTask)
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.exceptions import CallableReturnedInvalidObject, WorkflowRunKillSelf
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.client_logging import ClientLogging as logging
from jobmon.exceptions import DagLogRunningException

logger = logging.getLogger(__name__)


class JobListManager(object):

    def __init__(self, dag_id, executor=None, start_daemons=False,
                 job_instantiation_interval=10, n_queued_jobs=1000):
        """Once the job is ready to run and the task dag submits it, the job
        list manager takes over and monitors the job throughout its states
         until it is done or failed.

        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
                SequentialExecutor, DummyExecutor or SGEExecutor
            start_daemons (bool, default False): whether or not to start the
                JobInstanceFactory and JobReconciler as daemonized threads
            job_instantiation_interval (int, default 10): number of seconds to
                wait between instantiating newly ready jobs
            n_queued_jobs (int): number of queued jobs that should be returned
                to be instantiated
        """
        self.dag_id = dag_id
        self.job_factory = JobFactory(dag_id)

        self._stop_event = Event()
        self.job_instance_factory = JobInstanceFactory(
            dag_id=dag_id,
            executor=executor,
            n_queued_jobs=n_queued_jobs,
            stop_event=self._stop_event)
        self.job_inst_reconciler = JobInstanceReconciler(
            dag_id=dag_id,
            executor=executor,
            stop_event=self._stop_event)

        self.requester = shared_requester
        self.executor = executor

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
        """List of tasks that are listed as Registered, Done or Error_Fatal"""
        return [task for job_id, task in self.bound_tasks.items()
                if task.status not in [JobStatus.REGISTERED,
                                       JobStatus.DONE,
                                       JobStatus.ERROR_FATAL]]

    @property
    def workflow_run_id(self):
        if not hasattr(self, "_workflow_run_id"):
            raise AttributeError("workflow_run_id cannot be accessed before it"
                                 " has been assigned")
        return self._workflow_run_id

    @workflow_run_id.setter
    def workflow_run_id(self, val):
        logger.debug(f"Job List Manager has been assigned to workflow_run: "
                     f"{val}")
        self._workflow_run_id = val

    def bind_task(self, task: ExecutableTask):
        """Bind a task to the database, making it a job
        Args:
            task (obj): obj of a type inherited from ExecutableTask
        """
        if task.hash in self.hash_job_map:
            logger.info("Job already bound and has a hash, retrieving from db "
                        "and making sure updated parameters are bound")
            job = self.hash_job_map[task.hash]
            self._update_job(job.job_id, task.tag, task.max_attempts)
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

    def _bind_parameters(self, job_id, executor_parameter_set_type,
                         **kwargs):
        # evaluate the callable
        task = kwargs.get("task")
        resources = task.executor_parameters(kwargs)
        if not isinstance(resources, ExecutorParameters):
            raise CallableReturnedInvalidObject(f"The function called to "
                                                f"return resources did not "
                                                f"return the expected Executor"
                                                f" Parameters object, it is of"
                                                f" type {type(resources)}")
        task.bound_parameters.append(resources)

        if executor_parameter_set_type == ExecutorParameterSetType.VALIDATED:
            resources.validate()
        self._add_parameters(job_id, resources,
                             executor_parameter_set_type)

    def _add_parameters(self, job_id: int, executor_parameters,
                        parameter_set_type=ExecutorParameterSetType.VALIDATED):
        """Add an entry for the validated parameters to the database and
        activate them"""
        msg = {'parameter_set_type': parameter_set_type}
        msg.update(executor_parameters.to_wire())
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

    def update_f_jobs_in_dag_to_e(self, dag_id: int):
        self.requester.send_request(
            app_route=f'/task_dag/{dag_id}/update_jobs_status',
            message={"status_from": "F", "status_to": "E"},
            request_type="post"
        )

    def adjust_resources(self, task, *args, **kwargs):
        """Function from Job Instance Factory that adjusts resources and then
        queues them, this should also incorporate resource binding if they
        have not yet been bound"""
        logger.debug("Job in A state, adjusting resources before queueing")
        exec_param_set = task.bound_parameters[-1] # get the most recent parameter set
        only_scale = list(exec_param_set.resource_scales.keys())
        rc, msg = self.requester.send_request(
            app_route=f'/job/{task.job_id}/most_recent_ji_error',
            message={},
            request_type='get')
        if 'exceeded max_runtime' in msg and 'max_runtime_seconds' in only_scale:
            only_scale = ['max_runtime_seconds']
        logger.debug(f"only going to scale these resources: {only_scale}")
        resources_adjusted = {'only_scale': only_scale}
        exec_param_set.adjust(**resources_adjusted)
        return exec_param_set

    def log_dag_running(self) -> None:
        rc, msg = self.requester.send_request(
            app_route=f'/task_dag/{self.dag_id}/log_running',
            message={},
            request_type='post')
        if rc == StatusCodes.BAD_REQUEST:
            raise DagLogRunningException("Failed to log dag running. Received \"{}\" from server.".format(msg['message']))
        return rc

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

    def parse_adjusting_done_and_errors(self, jobs):
        """Separate out the done jobs from the errored ones
        Args:
            jobs(list): list of objects of type models.Job
        """
        completed_tasks = set()
        failed_tasks = set()
        adjusting_tasks = set()
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
            elif task.status == JobStatus.ADJUSTING_RESOURCES and task.is_bound:
                adjusting_tasks.add(task)
            else:
                continue
        self.all_done.update(completed_tasks)
        self.all_error -= completed_tasks
        self.all_error.update(failed_tasks)
        return completed_tasks, failed_tasks, adjusting_tasks

    def _sync(self):
        """Get all jobs from the database and parse the done and errored"""
        jobs = self.get_job_statuses()
        self.parse_adjusting_done_and_errors(jobs)

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
            workflow_run_resume_set = self._check_wfrun_resume_set()
            if workflow_run_resume_set:
                self.disconnect()
                raise WorkflowRunKillSelf(f"A resume has been set for this"
                                          f" workflow, workflow run "
                                          f"{self.workflow_run_id} will "
                                          f"disconnect its threads and exit")
            jobs = self.get_job_statuses()
            completed, failed, adjusting = self.parse_adjusting_done_and_errors(jobs)
            if adjusting:
                for task in adjusting:
                    # change callable to adjustment function
                    task.executor_parameters = partial(self.adjust_resources,
                                                       task)
                    self.adjust_resources_and_queue(task)
            if completed or failed:
                return completed, failed
            time.sleep(poll_interval)
            time_since_last_update += poll_interval

    def _check_wfrun_resume_set(self):
        rc, response = self.requester.send_request(
            app_route=f'/workflow_run/{self.workflow_run_id}/status',
            message={},
            request_type='get'
        )
        logger.debug(f"Status for workflow run {self.workflow_run_id} is "
                     f"{response}")
        if response == WorkflowRunStatus.COLD_RESUME:
            logger.info("A Cold Resume has been set, the JLM will qdel any job"
                        " instances and set them to kill themselves for "
                        "safety, then it will tear down the JIF and JIR and "
                        f"exit from workflow run {self.workflow_run_id}")
            exec_ids = self.requester.send_request(
                app_route=f'/workflow/{self.workflow_run_id}/job_instance_'
                          f'exec_ids',
                message={},
                request_type='get'
            )
            # qdel all job instances in this workflow run
            self.executor.terminate_all_jis_for_resume(exec_ids)

            # set all job instances to kill self
            rc, response = self.requester.send_request(
                app_route=f'/job_instance/{self.workflow_run_id}/'
                f'nonterminal_to_k_status',
                message={},
                request_type='post'
            )
            logger.debug("Job instances set to kill themselves if qdel failed "
                         "and they enter a running state")
            return True
        elif response == WorkflowRunStatus.HOT_RESUME:
            logger.info("A Hot Resume has been set, the JLM will leave any "
                        "running job instances so that they can complete, but "
                        "it will tear down the JIR and JIF threads and exit so"
                        " that no new job instances will be created for "
                        f"workflow run {self.workflow_run_id}")
            return True
        else:
            return False

    def _create_job(self, *args, **kwargs):
        """Create a job by passing the job args/kwargs through to the
        JobFactory
        """
        job = self.job_factory.create_job(*args, **kwargs)
        self.hash_job_map[job.job_hash] = job
        self.job_hash_map[job.job_id] = job.job_hash
        return job

    def adjust_resources_and_queue(self, task):
        """If a job is provided to the function then the resources have already
        been bound to the database and it should be set as adjusted"""
        job_id = self.hash_job_map[task.hash].job_id
        if not task.bound_parameters: # create O and V if no prior params are bound
            self._bind_parameters(job_id, ExecutorParameterSetType.ORIGINAL,
                                  task=task)
            self._bind_parameters(job_id, ExecutorParameterSetType.VALIDATED,
                                  task=task)
        else:
            self._bind_parameters(job_id, ExecutorParameterSetType.ADJUSTED,
                                  task=task)
        self.job_factory.queue_job(job_id)
        task = self.bound_tasks[job_id]
        task.status = JobStatus.QUEUED_FOR_INSTANTIATION

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
