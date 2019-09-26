from datetime import datetime
import getpass
import hashlib
from http import HTTPStatus as StatusCodes
import os
import uuid

from cluster_utils.io import makedirs_safely

import jobmon
from jobmon.client import shared_requester, client_config
from jobmon.models.attributes.constants import workflow_attribute
from jobmon.client.requester import Requester
from jobmon.client.swarm.workflow.workflow_run import WorkflowRun
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus, TaskDag
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client.client_logging import ClientLogging as logging


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

        self.task_dag = TaskDag(
            executor=self.executor,
            fail_fast=fail_fast,
            seconds_until_timeout=seconds_until_timeout
        )
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
        """Add a task to the workflow to be executed"""
        return self.task_dag.add_task(task)

    def add_tasks(self, tasks):
        """Add a list of task to the workflow to be executed"""
        self.task_dag.add_tasks(tasks)

    def _bind(self):
        """
        Bind the database and all of its tasks to the database.
        Also ensure that the task dag starts its threads, if this is the
        second or subsequent execution they will have been stopped.
        """
        if self.is_bound:
            self.task_dag.reconnect()
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
            self.task_dag.bind_to_db(
                self.dag_id,
                reset_running_jobs=self.reset_running_jobs
            )
        elif len(potential_wfs) == 0:
            # Bind the dag ...
            self.task_dag.bind_to_db(
                reset_running_jobs=self.reset_running_jobs
            )

            # Create new workflow in Database
            rc, response = self.requester.send_request(
                app_route='/workflow',
                message={'dag_id': str(self.task_dag.dag_id),
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
        hashval.update(
            bytes(self.task_dag.hash.encode('utf-8')))
        return hashval.hexdigest()

    def _create_workflow_run(self):
        """Create new workflow in the db"""
        self.workflow_run = WorkflowRun(
            self.id, self.stderr, self.stdout, self.project,
            executor_class=self.executor_class,
            reset_running_jobs=self.reset_running_jobs,
            working_dir=self.working_dir)

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
            message={'dag_hash': self.task_dag.hash},
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
        dag_status, n_new_done, n_prev_done, n_failed = (
            self.task_dag._execute_interruptible())

        if dag_status == DagExecutionStatus.SUCCEEDED:
            self._done()
        elif dag_status == DagExecutionStatus.FAILED:
            self._error()
        elif dag_status == DagExecutionStatus.STOPPED_BY_USER:
            self._stopped()
        else:
            raise RuntimeError("Received unknown response from "
                               "TaskDag._execute()")

        self.report(
            dag_status, n_new_done, n_prev_done, n_failed)
        return dag_status

    def report(self, dag_status, n_new_done, n_prev_done, n_failed):
        """Return the status of this workflow"""
        if dag_status == DagExecutionStatus.SUCCEEDED:
            logger.info(
                "Workflow finished successfully!")
            logger.info("# finished jobs: {}".format(
                n_new_done + n_prev_done))
        elif dag_status == DagExecutionStatus.FAILED:
            logger.info(
                "Workflow FAILED")
            logger.info(
                "# finished jobs (this run): {}".format(n_new_done))
            logger.info("# finished jobs (previous runs): {}"
                        .format(n_prev_done))
            logger.info(
                "# failed jobs: {}".format(n_failed))
        elif dag_status == DagExecutionStatus.STOPPED_BY_USER:
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
