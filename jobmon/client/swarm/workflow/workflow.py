from datetime import datetime
import getpass
import hashlib
from collections import OrderedDict
from http import HTTPStatus as StatusCodes
import os
from typing import Dict
import uuid

from cluster_utils.io import makedirs_safely

import jobmon
from jobmon.client import shared_requester, client_config
from jobmon.client.client_logging import ClientLogging as logging
from jobmon.client.requester import Requester
from jobmon.client.swarm.job_management.task_instance_state_controller import \
    TaskInstanceStateController
from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.client.swarm.workflow.bound_task import BoundTask
from jobmon.client.swarm.workflow.workflow_run import WorkflowRun, WorkflowRunExecutionStatus
from jobmon.exceptions import InvalidResponse
from jobmon.models.attributes.constants import workflow_attribute
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus


logger = logging.getLogger(__name__)


class WorkflowAlreadyComplete(Exception):
    pass


class WorkflowAlreadyExists(Exception):
    pass


class ResumeStatus(object):
    RESUME = True
    DONT_RESUME = False


class Workflow(object):
    """(aka Batch, aka Swarm)
    A Workflow is a framework by which a user may define the relationship
    between tasks and define the relationship between multiple runs of the same
    set of tasks. The great benefit of the Workflow is that it's resumable.
    A Workflow can only be re-loaded if two things are shown to be exact
    matches to a previous Workflow:

    1. WorkflowArgs: It is recommended to pass a meaningful unique identifier
        to workflow_args, to ease resuming. However, if the Workflow is a
        one-off project, you may instantiate the Workflow anonymously, without
        WorkflowArgs. Under the hood, the WorkflowArgs will default to a UUID
        which, as it is randomly generated, will be harder to remember and thus
        harder to resume.

        Workflow args must be hashable. For example, CodCorrect or Como version
        might be passed as Args to the Workflow. For now, the assumption is
        WorkflowArgs is a string.

    2. The tasks added to the workflow. A Workflow is built up by
        using Workflow.add_task(). In order to resume a Workflow, all the same
        tasks must be added with the same dependencies between tasks.
    """

    def __init__(self, workflow_args: str = None, name: str = "",
                 description: str = "", stderr: str = None, stdout: str = None,
                 project: str = None, reset_running_jobs: bool = True,
                 working_dir: str = None, executor_class: str = 'SGEExecutor',
                 fail_fast: bool = False,
                 requester: Requester = shared_requester,
                 seconds_until_timeout: int = 36000,
                 resume: bool = ResumeStatus.DONT_RESUME,
                 reconciliation_interval: int = None,
                 heartbeat_interval: int = None,
                 report_by_buffer: float = None):
        """
        Args:
            workflow_args: unique identifier of a workflow
            name: name of the workflow
            description: description of the workflow
            stderr: filepath where stderr should be sent, if run on SGE
            stdout: filepath where stdout should be sent, if run on SGE
            project: SGE project to run under, if run on SGE
            reset_running_jobs: whether or not to reset running jobs
            working_dir: the working dir that a job should be run from,
                if run on SGE
            executor_class: name of one of Jobmon's executors
            fail_fast: whether or not to break out of execution on
                first failure
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not and an identical workflow already exists, the
                workflow will error out
            reconciliation_interval: rate at which reconciler reconciles
                jobs to for errors and check state changes, default set to 10
                seconds in client config, but user can reconfigure here
            heartbeat_interval: rate at which worker node reports
                back if it is still alive and running
            report_by_buffer: number of heartbeats we push out the
                report_by_date (default = 3.1) so a job in qw can miss 3
                reconciliations or a running job can miss 3 worker heartbeats,
                and then we will register that it as lost
        """
        self.wf_dao = None
        self.name = name
        self.description = description

        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir
        self.executor_args = {
            'stderr': self.stderr,
            'stdout': self.stdout,
            'working_dir': self.working_dir,
            'project': self.project,
        }
        self.set_executor(executor_class)

        self.requester = requester

        self.reset_running_jobs = reset_running_jobs

        self.tasks = OrderedDict() # hash to task object mapping
        self.bound_tasks: Dict[int, BoundTask] = {} # hash to bound task object mapping
        self.fail_fast = fail_fast
        self.seconds_until_timeout = seconds_until_timeout

        self.job_instance_state_controller = None

        # self.task_dag = TaskDag(
        #     executor=self.executor,
        #     fail_fast=fail_fast,
        #     seconds_until_timeout=seconds_until_timeout
        # )
        self.resume = resume

        # if the user wants to specify the reconciliation and heartbeat rate,
        # as well as buffer time, they can do so here
        if reconciliation_interval:
            client_config.reconciliation_interval = reconciliation_interval
        if heartbeat_interval:
            client_config.heartbeat_interval = heartbeat_interval
        if report_by_buffer:
            client_config.report_by_buffer = report_by_buffer

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(
                uuid.uuid4())
            logger.info("Workflow_args defaulting to uuid {}. To resume this "
                        "workflow, you must re-instantiate Workflow and pass "
                        "this uuid in as the workflow_args. As a uuid is hard "
                        "to remember, we recommend you name your workflows and"
                        " make workflow_args a meaningful unique identifier. "
                        "Then add the same tasks to this workflow"
                        .format(self.workflow_args))

    def set_executor(self, executor_class):
        """Set which executor to use to run the tasks.

        Args:
            executor_class (str): string referring to one of the executor
            classes in jobmon.client.swarm.executors
        """
        self.executor_class = executor_class
        if self.executor_class == 'SGEExecutor':
            from jobmon.client.swarm.executors.sge import SGEExecutor
            self.executor = SGEExecutor(**self.executor_args)
        elif self.executor_class == "SequentialExecutor":
            from jobmon.client.swarm.executors.sequential import \
                SequentialExecutor
            self.executor = SequentialExecutor()
        elif self.executor_class == "DummyExecutor":
            from jobmon.client.swarm.executors.dummy import DummyExecutor
            self.executor = DummyExecutor()
        else:
            raise ValueError("{} is not a valid "
                             "executor_class".format(executor_class))

        if not hasattr(self.executor, "execute"):
            raise AttributeError(
                "Executor must have an execute() method")

    @property
    def dag_id(self):
        """Return the dag_id of this workflow"""
        if self.is_bound:
            return self.wf_dao.dag_id
        else:
            raise AttributeError(
                "Workflow is not yet bound")

    @property
    def hash(self):
        """Return the workflow hash of this workflow"""
        if self.is_bound:
            return self.wf_dao.workflow_hash
        else:
            raise AttributeError(
                "Workflow is not yet bound")

    @property
    def dag_hash(self):
        hashval = hashlib.sha1()
        for task_hash in sorted(self.tasks):
            hashval.update(bytes("{:x}".format(task_hash).encode('utf-8')))
            task = self.tasks[task_hash]
            for dtask in sorted(task.downstream_tasks):
                hashval.update(
                    bytes("{:x}".format(dtask.hash).encode('utf-8')))
        return hashval.hexdigest()

    @property
    def id(self):
        """Return the workflow_id of this workflow"""
        if self.is_bound:
            return self.wf_dao.id
        else:
            raise AttributeError(
                "Workflow is not yet bound")

    @property
    def is_bound(self):
        """Return a bool, whether or not this workflow is already bound to the
        db
        """
        if self.wf_dao:
            return True
        else:
            return False

    @property
    def status(self):
        """Return the status of this workflow"""
        if self.is_bound:
            return self.wf_dao.status
        else:
            raise AttributeError(
                "Workflow is not yet bound")

    def add_task(self, task):
        """Add a task to the workflow to be executed.
           Set semantics - add tasks once only, based on hash name. Also
           creates the job. If is_no has no task_id the creates task_id and
           writes it onto object.
        """
        logger.debug(f"Adding Task {task}")
        if task.hash in self.tasks:
            raise ValueError(f"A task with hash {task.hash} already exists. "
                             f"All tasks in a workflow must have unique "
                             f"commands. Your command was: {task.command}")
        self.tasks[task.hash] = task
        logger.debug(f"Task {task.hash} added")

        return task

    def add_tasks(self, tasks):
        """Add a list of task to the workflow to be executed"""
        for task in tasks:
            self.add_task(task)

    def _create_dag(self):
        """Creates a new DAG"""
        logger.debug("Creating new dag")
        rc, response = self.requester.send_request(
            app_route=f'/client_dag/{self.dag_hash}',
            request_type='post')
        dag_id = response['dag_id']
        logger.debug(f"New dag created with id: {self.dag_id}")
        return dag_id

    def _bind_dag(self):
        """
        Binds the dag to the database
        :return:
        """
        if self.dag_id:
            for _, task in self.tasks.items():
                self._bind_task(task, self.dag_id)
            if self.reset_running_jobs:
                self._reset_tasks()
        else:
            dag_id = self._create_dag()
            for _, task in self.tasks.items():
                self._bind_task(task, dag_id)
            return dag_id

    def _bind_task(self, task: ExecutableTask, dag_id: int):
        if task.hash in self.bound_tasks:
            logger.info("Task already bound and has a hash, retrieving from "
                        "db and making sure updated parameters are bound")
            bound_task = self.bound_tasks[task.hash]
            bound_task.update_task(task.max_attempts)
        else:
            swarm_task = task.create_bound_task(dag_id, self.requester)
            bound_task = BoundTask(client_task=task, bound_task=swarm_task,
                                   requester=self.requester)
            # using sets so that a bound task will only be added if it is not
            # already there
            for upstream in task.upstream_tasks:
                if upstream.hash in self.bound_tasks:
                    bound_task.upstream_bound_tasks.add(self.bound_tasks[upstream.hash])
                    self.bound_tasks[upstream.hash].downstream_bound_tasks.add(bound_task)
            self.bound_tasks[bound_task.hash] = bound_task

        for attribute in task.job_attributes:
            logger.info(f"Add job attribute for task_id : {bound_task.task_id}"
                        f", attribute_type: {attribute}, value: "
                        f"{task.job_attributes[attribute]}")
            bound_task.add_task_attribute(attribute,
                                          task.job_attributes[attribute])
        return bound_task

    def _reset_tasks(self):
        """Reset all incomplete jobs of a dag_id, identified by self.dag_id"""
        logger.info(f"Reset tasks for dag_id {self.dag_id}")
        rc, _ = self.requester.send_request(
            app_route='/task_dag/{}/reset_incomplete_tasks'.format(self.dag_id),
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(f"{rc}: Could not reset tasks")
        return rc

    def _bind(self):
        """
        Bind the database and all of its tasks to the database.
        Also ensure that the task dag starts its threads, if this is the
        second or subsequent execution they will have been stopped.
        """
        if self.is_bound:
            self.job_instance_state_controller.connect()
        self._matching_wf_args_diff_hash()
        potential_wfs = self._matching_workflows()
        if len(potential_wfs) > 0 and not self.resume:
            raise WorkflowAlreadyExists("This workflow and task dag already "
                                        "exist. If you are trying to resume a "
                                        "workflow, please set the resume flag "
                                        "of the workflow. If you are not "
                                        "trying to resume a workflow, make "
                                        "sure the workflow args are unique or "
                                        "the task dag is unique ")
        elif len(potential_wfs) > 0 and self.resume:
            logger.info("Your workflow args and task dag are identical to a "
                        "prior workflow, therefore you are RESUMING a "
                        "workflow, not starting a new workflow")
            self.wf_dao = potential_wfs[
                0]
            if self.wf_dao.status == WorkflowStatus.DONE:
                raise WorkflowAlreadyComplete

            # bind tasks with existing dag id
            self._bind_dag()
        elif len(potential_wfs) == 0:
            # Create and bind the new dag ...
            dag_id = self._bind_dag()

            # Create new workflow in Database
            rc, response = self.requester.send_request(
                app_route='/workflow',
                message={'dag_id': str(self.dag_id),
                         'workflow_args': self.workflow_args,
                         'workflow_hash': self._compute_hash(),
                         'name': self.name,
                         'description': self.description,
                         'user': getpass.getuser()},
                request_type='post')
            self.wf_dao = WorkflowDAO.from_wire(response['workflow_dct'])
        else:
            raise RuntimeError("Multiple matching Workflows found {}. "
                               "Workflows should be unique on TaskDag and "
                               "WorkflowArgs".format(potential_wfs))
        if not self.job_instance_state_controller:
            self.job_instance_state_controller = TaskInstanceStateController(
                dag_id=self.dag_id, workflow_id=self.id, executor=self.executor, start_daemons=True)

    def _done(self):
        """Update the workflow as done"""
        self.workflow_run.update_done()
        self._update_status(
            WorkflowStatus.DONE)

    def _compute_hash(self):
        """Create a unique hash for this workflow"""
        hashval = hashlib.sha1()
        hashval.update(
            bytes(self.workflow_args.encode('utf-8')))
        hashval.update(bytes(self.dag_hash.encode('utf-8')))
        return hashval.hexdigest()

    def _create_workflow_run(self):
        """Create new workflow in the db"""
        self.workflow_run = WorkflowRun(
            self.id, self.dag_id, self.stderr, self.stdout, self.project,
            self.job_instance_state_controller,
            executor_class=self.executor_class,
            reset_running_jobs=self.reset_running_jobs,
            working_dir=self.working_dir,
            requester=self.requester,
            bound_tasks=self.bound_tasks,
            fail_fast=self.fail_fast,
            seconds_until_timeout=self.seconds_until_timeout)

    def _error(self):
        """Update the workflow as errored"""
        self.workflow_run.update_error()
        self._update_status(
            WorkflowStatus.ERROR)

    def _stopped(self):
        """Update the workflow as stopped"""
        self.workflow_run.update_stopped()
        self._update_status(WorkflowStatus.STOPPED)

    def _matching_wf_args_diff_hash(self):
        """Check """
        workflow_hash = self._compute_hash()
        rc, response = self.requester.send_request(
            app_route='/workflow/workflow_args',
            message={'workflow_args': str(self.workflow_args)},
            request_type='get')
        bound_workflow_hashes = response['workflow_hashes']
        for hash in bound_workflow_hashes:
            if workflow_hash != hash[0]:
                raise WorkflowAlreadyExists("The unique workflow_args already"
                                            " belong to a workflow that "
                                            "contains different tasks than the"
                                            " workflow you are creating, "
                                            "either change your workflow args "
                                            "so that they are unique for this "
                                            "set of tasks, or make sure your "
                                            "tasks match the workflow you are "
                                            "trying to resume")

    def _matching_dag_ids(self):
        """Find all matching dag_ids for this task_dag_hash"""
        rc, response = self.requester.send_request(
            app_route='/dag',
            message={'dag_hash': self.dag_hash},
            request_type='get')
        dag_ids = response['dag_ids']
        return dag_ids

    def _matching_workflows(self):
        """Find all matching workflows that have the same task_dag_hash"""
        dag_ids = self._matching_dag_ids()
        workflows = []
        for dag_id in dag_ids:
            rc, response = self.requester.send_request(
                app_route='/dag/{}/workflow'.format(dag_id),
                message={'workflow_args': str(self.workflow_args)},
                request_type='get')
            if rc == StatusCodes.OK:
                wf = response['workflow_dct']
                workflows.append(wf)
        return [WorkflowDAO.from_wire(w) for w in workflows]

    def _update_status(self, status):
        """Update the workflow with the status passed in"""
        rc, response = self.requester.send_request(
            app_route='/workflow',
            message={'wf_id': str(self.id), 'status': status,
                     'status_date': str(datetime.utcnow())},
            request_type='put')
        wf_dct = response['workflow_dct']
        self.wf_dao = WorkflowDAO.from_wire(wf_dct)

    def _set_executor_temp_dir(self):
        """Create a temp_dir for the executor. This is primarily needed for
        StataTask, since it creates logs in the working dir by default,
        potentially overwhelming that directory
        """
        scratch_tmp_dir = "/ihme/scratch/tmp/jobmon/stata_logs"
        local_tmp_dir = "/tmp"
        if os.path.exists(os.path.realpath(scratch_tmp_dir)):
            tmp_root = scratch_tmp_dir
        elif os.path.exists(os.path.realpath(local_tmp_dir)):
            tmp_root = local_tmp_dir
        else:
            raise FileNotFoundError("Could not locate a valid root temporary "
                                    "directory. Neither {} nor {} is "
                                    "available".format(scratch_tmp_dir,
                                                       local_tmp_dir))
        tmp_dir = os.path.join(tmp_root,
                               jobmon.__version__,
                               str(self.wf_dao.id),
                               str(self.workflow_run.id))
        logger.info(
            "Creating temp directory at {}".format(tmp_dir))
        makedirs_safely(tmp_dir)
        self.executor.set_temp_dir(
            tmp_dir)

    def execute(self) -> int:
        """Run this workflow"""
        # add_jobmon_file_logger('jobmon', logging.DEBUG,
        #                        '{}/jobmon.log'.format(os.getcwd()))

        if not self.is_bound:
            self._bind()
        self._create_workflow_run()
        self._set_executor_temp_dir()
        wfr_status, n_new_done, n_prev_done, n_failed = (
            self.workflow_run.execute_interruptible())

        if wfr_status == WorkflowRunExecutionStatus.SUCCEEDED:
            self._done()
        elif wfr_status == WorkflowRunExecutionStatus.FAILED:
            self._error()
        elif wfr_status == WorkflowRunExecutionStatus.STOPPED_BY_USER:
            self._stopped()
        else:
            raise RuntimeError("Received unknown response from "
                               "WorkflowRun._execute()")

        self.report(
            wfr_status, n_new_done, n_prev_done, n_failed)
        return wfr_status

    def report(self, dag_status, n_new_done, n_prev_done, n_failed):
        """Return the status of this workflow"""
        if dag_status == WorkflowRunExecutionStatus.SUCCEEDED:
            logger.info(
                "Workflow finished successfully!")
            logger.info("# finished jobs: {}".format(
                n_new_done + n_prev_done))
        elif dag_status == WorkflowRunExecutionStatus.FAILED:
            logger.info(
                "Workflow FAILED")
            logger.info(
                "# finished jobs (this run): {}".format(n_new_done))
            logger.info("# finished jobs (previous runs): {}"
                        .format(n_prev_done))
            logger.info(
                "# failed jobs: {}".format(n_failed))
        elif dag_status == WorkflowRunExecutionStatus.STOPPED_BY_USER:
            logger.info(
                "Workflow STOPPED_BY_USER")
            logger.info(
                "# finished jobs: {}", n_new_done)

    def run(self):
        """Alias for self.execute"""
        return self.execute()

    def is_valid_attribute(self, attribute_type, value):
        """
        - attribute_type has to be an int
        - for now, value can only be str or int
        - value has to be int or convertible to int,
          except when the attribute_type is a tag
        - value can be any string when attribute_type is a tag

        Args:
            attribute_type (int): attribute_type id from
                                   workflow_run_attribute_type table
            value (int): value associated with attribute
        Returns:
            True (or raises)
        Raises:
            ValueError: if the args for add_attribute is not valid.
        """
        if not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif not attribute_type == workflow_attribute.TAG and not int(value):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        return True

    def add_workflow_attribute(self, attribute_type, value):
        """Create workflow attribute entry in workflow_attribute table"""
        self.is_valid_attribute(
            attribute_type, value)
        if self.is_bound:
            rc, response = self.requester.send_request(
                app_route='/workflow_attribute',
                message={'workflow_id': str(self.id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return response['workflow_attribute_id']
        else:
            raise AttributeError(
                "Workflow is not yet bound")
