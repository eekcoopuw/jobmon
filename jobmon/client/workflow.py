import hashlib
from http import HTTPStatus as StatusCodes
from typing import Optional, List, Tuple
import uuid

from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.dag import Dag
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.task import Task
from jobmon.exceptions import WorkflowAlreadyExists, WorkflowAlreadyComplete
from jobmon.models.workflow_status import WorkflowStatus


logger = logging.getLogger(__name__)


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

    def __init__(self,
                 tool_version_id: int,
                 workflow_args: Optional[str] = None,
                 name: str = "",
                 description: str = "",
                 requester: Requester = shared_requester):
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
        self.tool_version_id = tool_version_id
        self.name = name
        self.description = description

        self._dag = Dag()
        # hash to task object mapping. ensure only 1
        self.tasks: dict = {}

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(uuid.uuid4())
            logger.info("Workflow_args defaulting to uuid {}. To resume this "
                        "workflow, you must re-instantiate Workflow and pass "
                        "this uuid in as the workflow_args. As a uuid is hard "
                        "to remember, we recommend you name your workflows and"
                        " make workflow_args a meaningful unique identifier. "
                        "Then add the same tasks to this workflow"
                        .format(self.workflow_args))
        self.workflow_args_hash = int(
            hashlib.sha1(self.workflow_args.encode('utf-8')).hexdigest(), 16)

        self.requester = requester

    @property
    def is_bound(self):
        if not hasattr(self, "_workflow_id"):
            return False
        else:
            return True

    @property
    def workflow_id(self) -> int:
        if not self.is_bound:
            raise AttributeError(
                "workflow_id cannot be accessed before workflow is bound")
        return self._workflow_id

    @property
    def dag_id(self) -> int:
        if not self.is_bound:
            raise AttributeError(
                "dag_id cannot be accessed before workflow is bound")
        return self._dag.dag_id

    @property
    def task_hash(self):
        hash_value = hashlib.sha1()
        tasks = sorted(self.tasks.values())
        if len(tasks) > 0:  # if there are no tasks, we want to skip this
            for task in tasks:
                hash_value.update(str(hash(task)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)

    def add_task(self, task: Task):
        """Add a task to the workflow to be executed.
           Set semantics - add tasks once only, based on hash name. Also
           creates the job. If is_no has no task_id the creates task_id and
           writes it onto object.
        """
        logger.debug(f"Adding Task {task}")
        if hash(task) in self.tasks.keys():
            raise ValueError(f"A task with hash {hash(task)} already exists. "
                             f"All tasks in a workflow must have unique "
                             f"commands. Your command was: {task.command}")
        self.tasks[hash(task)] = task
        self._dag.add_node(task.node)
        logger.debug(f"Task {hash(task)} added")

        return task

    def add_tasks(self, tasks: List[Task]):
        """Add a list of task to the workflow to be executed"""
        for task in tasks:
            self.add_task(task)

    def _bind(self,
              resume: bool = ResumeStatus.DONT_RESUME,
              reset_running_jobs: bool = True):
        # short circuit if already bound
        if self.is_bound:
            return

        # check if workflow is valid
        self._dag.validate()  # this does nothing at the moment
        self._matching_wf_args_diff_hash()

        # bind structural elements to db
        for node in self._dag.nodes:
            node.bind()
        self._dag.bind()

        # bind workflow
        workflow_id, status = self._get_workflow_id_and_status()

        # raise error if workflow exists and is done
        if status == WorkflowStatus.DONE:
            raise WorkflowAlreadyComplete

        if workflow_id is not None:
            if resume:
                if status == WorkflowStatus.RUNNING:
                    # TODO
                    # tell a previous workflow_run to kill itself.
                    # go into wait loop waiting for workflow to move to error.
                    # consider pulling code from
                    # workflow_run.kill_previous_workflow_runs
                    raise NotImplementedError
                if status == WorkflowStatus.CREATED:
                    # TODO
                    # here we can directly set the workflow state because the
                    # workflow_run hasn't started yet. There could be races so
                    # needs to be carefully designed
                    raise NotImplementedError

                # TODO
                # what happens if the workflow is in other states? what about
                # if the previous workflow belongs to another user?
            else:
                raise WorkflowAlreadyExists(
                    "This workflow already exist. If you are trying to "
                    "resume a workflow, please set the resume flag  of the"
                    " workflow. If you are not trying to resume a "
                    "workflow, make sure the workflow args are unique or "
                    "the tasks are unique")
        else:
            workflow_id = self._add_workflow()
            status = WorkflowStatus.REGISTERED
        self._workflow_id = workflow_id

        # add tasks to workflow
        try:
            # TODO: confirm executor parameters executor class matches
            # job instance state controller executor type

            for task in self.tasks.values():
                task.workflow_id = self.workflow_id
                task.bind()
        except Exception:
            # set to aborted unless we are getting 500s
            pass
        else:
            # set to bound unless we are getting 500s
            pass

    def run(self,
            fail_fast: bool = False,
            seconds_until_timeout: int = 36000,
            resume: bool = ResumeStatus.DONT_RESUME,
            reset_running_jobs: bool = True):

        # bind to database
        self._bind(resume, reset_running_jobs)

        raise ValueError
        # create swarmtasks
        swarm_tasks = []
        for task in self.tasks.values():
            swarm_tasks.append(self._create_swarm_task(task))

        # create workflow_run and execute it
        wfr = self._create_workflow_run()
        wfr.execute_interruptible()

    def _matching_wf_args_diff_hash(self):
        """Check """
        rc, response = self.requester.send_request(
            app_route=f'/workflow/{str(self.workflow_args_hash)}',
            message={},
            request_type='get')
        bound_workflow_hashes = response['matching_workflows']
        for task_hash, tool_version_id, dag_hash in bound_workflow_hashes:
            match = (
                self.task_hash == task_hash and self.tool_version_id and
                hash(self.dag) == dag_hash)
            if match:
                raise WorkflowAlreadyExists(
                    "The unique workflow_args already belong to a workflow "
                    "that contains different tasks than the workflow you are "
                    "creating, either change your workflow args so that they "
                    "are unique for this set of tasks, or make sure your tasks"
                    " match the workflow you are trying to resume")

    def _get_workflow_id_and_status(self) -> Tuple[Optional[int],
                                                   Optional[str]]:
        return_code, response = self.requester.send_request(
            app_route='/workflow',
            message={
                "tool_version_id": self.tool_version_id,
                "dag_id": self._dag.dag_id,
                "workflow_args_hash": self.workflow_args_hash,
                "task_hash": self.task_hash
            },
            request_type='get'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /workflow. Expected code '
                             f'200. Response content: {response}')
        return response['workflow_id'], response["status"]

    def _add_workflow(self) -> int:
        app_route = f'/workflow'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "tool_version_id": self.tool_version_id,
                "dag_id": self._dag.dag_id,
                "workflow_args_hash": self.workflow_args_hash,
                "task_hash": self.task_hash,
                "description": self.description,
                "name": self.name,
                "workflow_args": self.workflow_args
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from PUT '
                             f'request through route {app_route}. Expected '
                             f'code 200. Response content: {response}')
        return response["workflow_id"]

    def _create_swarm_task(self, task: Task) -> SwarmTask:
        swarm_task = SwarmTask(
            task_id=task.task_id,
            status=task.status,
            task_args_hash=task.task_args_hash,
            executor_parameters=task.executor_parameters,
            max_attempts=task.max_attempts)
        return swarm_task

    def _create_workflow_run(self) -> WorkflowRun:
        return WorkflowRun(
            workflow_id=self.workflow_id)

    def __hash__(self):
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.tool_version_id)).encode('utf-8'))
        hash_value.update(str(self.workflow_args_hash).encode('utf-8'))
        hash_value.update(str(self.task_hash).encode('utf-8'))
        hash_value.update(str(hash(self._dag)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)

    # def _reset_tasks(self):
    #     """Reset all incomplete jobs of a dag_id, identified by self.dag_id"""
    #     logger.info(f"Reset tasks for dag_id {self.dag_id}")
    #     rc, _ = self.requester.send_request(
    #         app_route='/task_dag/{}/reset_incomplete_tasks'.format(self.dag_id),
    #         message={},
    #         request_type='post')
    #     if rc != StatusCodes.OK:
    #         raise InvalidResponse(f"{rc}: Could not reset tasks")
    #     return rc

    # def _update_status(self, status):
    #     """Update the workflow with the status passed in"""
    #     rc, response = self.requester.send_request(
    #         app_route='/workflow',
    #         message={'wf_id': str(self.id), 'status': status,
    #                  'status_date': str(datetime.utcnow())},
    #         request_type='put')
    #     wf_dct = response['workflow_dct']
    #     self.wf_dao = WorkflowDAO.from_wire(wf_dct)

    # def report(self, dag_status, n_new_done, n_prev_done, n_failed):
    #     """Return the status of this workflow"""
    #     if dag_status == WorkflowRunExecutionStatus.SUCCEEDED:
    #         logger.info(
    #             "Workflow finished successfully!")
    #         logger.info("# finished jobs: {}".format(
    #             n_new_done + n_prev_done))
    #     elif dag_status == WorkflowRunExecutionStatus.FAILED:
    #         logger.info(
    #             "Workflow FAILED")
    #         logger.info(
    #             "# finished jobs (this run): {}".format(n_new_done))
    #         logger.info("# finished jobs (previous runs): {}"
    #                     .format(n_prev_done))
    #         logger.info(
    #             "# failed jobs: {}".format(n_failed))
    #     elif dag_status == WorkflowRunExecutionStatus.STOPPED_BY_USER:
    #         logger.info(
    #             "Workflow STOPPED_BY_USER")
    #         logger.info(
    #             "# finished jobs: {}", n_new_done)
