import hashlib
from http import HTTPStatus as StatusCodes
from multiprocessing import Process, Event, Queue
from multiprocessing import synchronize
from queue import Empty
from typing import Optional, Sequence, Tuple, Dict, Union, List
import uuid

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.dag import Dag
from jobmon.client.execution.scheduler.execution_config import ExecutionConfig
from jobmon.client.execution.scheduler.task_instance_scheduler import \
    TaskInstanceScheduler, ExceptionWrapper
from jobmon.client.execution.strategies.base import Executor
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.task import Task
from jobmon.exceptions import (WorkflowAlreadyExists, WorkflowAlreadyComplete,
                               InvalidResponse, SchedulerStartupTimeout,
                               SchedulerNotAlive, ResumeSet)
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.task_status import TaskStatus


logger = logging.getLogger(__name__)


class ResumeStatus(object):
    """Enum of allowed resume statuses."""

    RESUME = True
    DONT_RESUME = False


class Workflow(object):
    """(aka Batch, aka Swarm).

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

    def __init__(self, tool_version_id: int, workflow_args: str = "",
                 name: str = "", description: str = "",
                 requester: Requester = shared_requester,
                 workflow_attributes: Union[List, dict] = None):
        """
        Args:
            tool_version_id: id of the associated tool 
            workflow_args: unique identifier of a workflow
            name: name of the workflow
            description: description of the workflow
            requester: the requester used to communicate with central services.
            workflow_attributes: attributes that make this workflow different
                from other workflows that the user wants to record.
        """
        self.tool_version_id = tool_version_id
        self.name = name
        self.description = description

        self._dag = Dag()
        # hash to task object mapping. ensure only 1
        self.tasks: Dict[int, Task] = {}
        self._swarm_tasks: dict = {}

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
        self._scheduler_proc: Optional[Process] = None
        self._scheduler_com_queue: Queue = Queue()
        self._scheduler_stop_event: synchronize.Event = Event()
        self.workflow_attributes = {}
        if workflow_attributes:
            if isinstance(workflow_attributes, List):
                for attr in workflow_attributes:
                    self.workflow_attributes[attr] = None
            elif isinstance(workflow_attributes, dict):
                for attr, val in workflow_attributes.items():
                    self.workflow_attributes[str(attr)] = str(val)
            else:
                raise ValueError("workflow_attributes must be provided as a "
                                 "list of attributes or a dictionary of"
                                 " attributes and their values")

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

    def add_attributes(self, workflow_attributes: dict) -> None:
        """Function that users can call either to update values of existing
        attributes or add new attributes

        Args:
            workflow_attributes: attributes to be bound to the db that describe
                this workflow.
        """

        app_route = f'/client/workflow/{self.workflow_id}/workflow_attributes'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"workflow_attributes": workflow_attributes},
            request_type="put"
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')

    def add_task(self, task: Task) -> Task:
        """Add a task to the workflow to be executed.
           Set semantics - add tasks once only, based on hash name. Also
           creates the job. If is_no has no task_id the creates task_id and
           writes it onto object.

           Args:
               task: single task to add
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

    def add_tasks(self, tasks: Sequence[Task]):
        """Add a list of task to the workflow to be executed"""
        for task in tasks:
            self.add_task(task)

    def set_executor(self, executor: Executor = None,
                     execution_config: ExecutionConfig =
                     ExecutionConfig.from_defaults(),
                     executor_class: Optional[str] = 'SGEExecutor', *args,
                     **kwargs):
        """Set the executor and any arguments specific to that executor that
        will be applied to the entire workflow (ex. specify project here for
        SGEExecutor class).

        Args:
            executor: if an executor object has already been created, use it
            execution_config: configuration of scheduling settings and other
                execution related settings
            executor_class: which executor to run your tasks on
        """
        self._executor: 'Executor'
        self._execution_config: ExecutionConfig = execution_config

        if executor is not None:
            self._executor = executor
        else:
            if executor_class == "SequentialExecutor":
                from jobmon.client.execution.strategies.sequential import \
                    SequentialExecutor as Executor
            elif executor_class == "SGEExecutor":
                from jobmon.client.execution.strategies.sge.sge_executor import \
                    SGEExecutor as Executor
            elif executor_class == "DummyExecutor":
                from jobmon.client.execution.strategies.dummy import \
                    DummyExecutor as Executor
            elif executor_class == "MultiprocessExecutor":
                from jobmon.client.execution.strategies.multiprocess import \
                    MultiprocessExecutor as Executor
            else:
                raise ValueError(f"{executor_class} is not a valid ExecutorClass")
            self._executor = Executor(*args, **kwargs)

    def run(self,
            fail_fast: bool = False,
            seconds_until_timeout: int = 36000,
            resume: bool = ResumeStatus.DONT_RESUME,
            reset_running_jobs: bool = True,
            scheduler_response_wait_timeout=180) -> WorkflowRun:
        """Run the workflow by traversing the dag and submitting new tasks when
        their tasks have completed successfully.
        Args:
            fail_fast: whether or not to break out of execution on
                first failure
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not set to resume and an identical workflow already
                exists, the workflow will error out
            reset_running_jobs: whether or not to reset running jobs upon resume
            scheduler_response_wait_timeout: amount of time to wait for the
                scheduler thread to start up

        Returns:
            WorkflowRun: object of WorkflowRun, can be checked to make sure all
                jobs ran to completion, checked for status, etc.
        """
        if not hasattr(self, "_executor"):
            logger.warning("using default project: ihme_general")
            self.set_executor(project="ihme_general")
        logger.debug("executor: {}".format(self._executor))

        # bind to database
        self._bind(resume)

        # create workflow_run
        wfr = self._create_workflow_run(resume, reset_running_jobs)

        # testing parameter
        if hasattr(self, "_val_fail_after_n_executions"):
            wfr._set_fail_after_n_executions(self._val_fail_after_n_executions)

        try:
            # start scheduler
            scheduler_proc = self._start_task_instance_scheduler(
                wfr.workflow_run_id, scheduler_response_wait_timeout)
            # execute the workflow run
            wfr.execute_interruptible(scheduler_proc, fail_fast,
                                      seconds_until_timeout)
            logger.info(f"Scheduler started up successfully and the workflow "
                        f"run finished executing. Workflow Run status is: "
                        f"{wfr.status}")
            return wfr

        except KeyboardInterrupt:
            wfr.update_status(WorkflowRunStatus.STOPPED)
            logger.warning("Keyboard interrupt raised and Workflow Run set to "
                           "Stopped")
            return wfr

        except SchedulerNotAlive:
            # check if we got an exception from the scheduler
            try:
                resp = self._scheduler_com_queue.get(False)
            except Empty:
                wfr.update_status(WorkflowRunStatus.ERROR)
                # no response. raise scheduler not alive
                raise
            else:
                # re-raise error from scheduler
                if isinstance(resp, ExceptionWrapper):
                    try:
                        resp.re_raise()
                    except ResumeSet:
                        # if the exception was a resume exception we set to
                        # terminate
                        wfr.terminate_workflow_run()
                        raise
                    except Exception:
                        wfr.update_status(WorkflowRunStatus.ERROR)
                        raise
                else:
                    # response is not an exception
                    wfr.update_status(WorkflowRunStatus.ERROR)
                    raise
            finally:
                scheduler_proc.terminate()
                self._scheduler_proc = None

        except Exception:
            wfr.update_status(WorkflowRunStatus.ERROR)
            raise

        finally:
            # deal with task instance scheduler process if it was started
            if self._scheduler_proc is not None:
                self._scheduler_stop_event.set()
                try:
                    # give it some time to shut down
                    self._scheduler_com_queue.get(
                        timeout=scheduler_response_wait_timeout)
                except Empty:
                    pass
                self._scheduler_proc.terminate()

    def _bind(self, resume: bool = ResumeStatus.DONT_RESUME):
        """Bind objects to the database if they haven't already been"""
        # short circuit if already bound
        if self.is_bound:
            return

        # check if workflow is valid
        self._dag.validate()  # TODO: this does nothing at the moment
        self._matching_wf_args_diff_hash()

        # bind structural elements to db
        for node in self._dag.nodes:
            node.bind()
        self._dag.bind()

        # bind workflow
        workflow_id, status = self._get_workflow_id_and_status()

        # raise error if workflow exists and is done
        if status == WorkflowStatus.DONE:
            raise WorkflowAlreadyComplete(
                f"Workflow id ({workflow_id}) is already in done state and "
                "cannot be resumed")

        if workflow_id is not None:
            if not resume:
                raise WorkflowAlreadyExists(
                    "This workflow already exist. If you are trying to "
                    "resume a workflow, please set the resume flag  of the"
                    " workflow. If you are not trying to resume a "
                    "workflow, make sure the workflow args are unique or "
                    "the tasks are unique")

            # Add workflow attributes and workflow_id
            self._workflow_id = workflow_id
            self.add_attributes(self.workflow_attributes)
        else:
            workflow_id = self._add_workflow()
            self._workflow_id = workflow_id

    def _matching_wf_args_diff_hash(self):
        """Check that an existing workflow with the same workflow_args does not
         have a different hash indicating that it contains different tasks."""
        rc, response = self.requester.send_request(
            app_route=f'/client/workflow/{str(self.workflow_args_hash)}',
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
            app_route='/client/workflow',
            message={
                "tool_version_id": self.tool_version_id,
                "dag_id": self._dag.dag_id,
                "workflow_args_hash": self.workflow_args_hash,
                "task_hash": self.task_hash
            },
            request_type='get'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from GET '
                f'request through route /client/workflow. Expected code '
                f'200. Response content: {response}')
        return response['workflow_id'], response["status"]

    def _add_workflow(self) -> int:
        app_route = f'/client/workflow'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "tool_version_id": self.tool_version_id,
                "dag_id": self._dag.dag_id,
                "workflow_args_hash": self.workflow_args_hash,
                "task_hash": self.task_hash,
                "description": self.description,
                "name": self.name,
                "workflow_args": self.workflow_args,
                "workflow_attributes": self.workflow_attributes
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
        return response["workflow_id"]

    def _bind_tasks(self, reset_if_running: bool = True):
        app_route = f'/client/task/bind_tasks'
        parameters = {}
        # send to server in a format of:
        # {<hash>:[workflow_id(0), node_id(1), task_args_hash(2), name(3),
        # command(4), max_attempts(5)], reset_if_running(6), task_args(7), task_attributes(8)}
        # flat the data structure so that the server won't depend on the client
        tasks = {}
        for k in self.tasks.keys():
            tasks[k] = [self.workflow_id, self.tasks[k].node.node_id, self.tasks[k].task_args_hash,
                        self.tasks[k].name, self.tasks[k].command, self.tasks[k].max_attempts,
                        reset_if_running, self.tasks[k].task_args, self.tasks[k].task_attributes]
        parameters = {"tasks": tasks}
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=parameters,
            request_type='put'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from PUT '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')
        return_tasks = response["tasks"]
        for k in return_tasks.keys():
            self.tasks[int(k)].task_id = return_tasks[k][0]
            self.tasks[int(k)].initial_status = return_tasks[k][1]

    def _create_workflow_run(self, resume: bool = ResumeStatus.DONT_RESUME,
                             reset_running_jobs: bool = True) -> WorkflowRun:
        swarm_tasks: Dict[int, SwarmTask] = {}
        # create workflow run
        wfr = WorkflowRun(
            workflow_id=self.workflow_id,
            executor_class=self._executor.__class__.__name__,
            resume=resume,
            reset_running_jobs=reset_running_jobs,
            requester=self.requester)

        try:
            self._bind_tasks(reset_running_jobs)
            for task in self.tasks.values():
                # create swarmtasks
                swarm_task = SwarmTask(
                    task_id=task.task_id,
                    status=task.initial_status,
                    task_args_hash=task.task_args_hash,
                    executor_parameters=task.executor_parameters,
                    max_attempts=task.max_attempts)
                swarm_tasks[task.task_id] = swarm_task

            # create relationships on swarm tasks
            for task in self.tasks.values():
                swarm_task = swarm_tasks[task.task_id]
                swarm_task.upstream_swarm_tasks = set([
                    swarm_tasks[t.task_id] for t in task.upstream_tasks])
                swarm_task.downstream_swarm_tasks = set([
                    swarm_tasks[t.task_id] for t in task.downstream_tasks])

            # add swarm tasks to workflow run
            wfr.swarm_tasks = swarm_tasks
        except Exception:
            # update status then raise
            wfr.update_status(WorkflowRunStatus.ABORTED)
            raise
        else:
            wfr.update_status(WorkflowRunStatus.BOUND)

        return wfr

    def _start_task_instance_scheduler(self, workflow_run_id: int,
                                       scheduler_startup_wait_timeout: int
                                       ) -> Process:

        # instantiate scheduler and launch in separate proc. use event to
        # signal back when scheduler is started
        scheduler = TaskInstanceScheduler(self.workflow_id, workflow_run_id,
                                          self._executor,
                                          self._execution_config)
        try:
            scheduler_proc = Process(target=scheduler.run_scheduler,
                                     args=[self._scheduler_stop_event,
                                           self._scheduler_com_queue])
            scheduler_proc.start()

            # wait for response from scheduler
            resp = self._scheduler_com_queue.get(
                timeout=scheduler_startup_wait_timeout)
        except Empty:  # mypy complains but this is correct
            raise SchedulerStartupTimeout(
                "Scheduler process did not start within the alloted timeout "
                f"t={scheduler_startup_wait_timeout}s")
        else:
            # the first message can only be "ALIVE" or an ExceptionWrapper
            if isinstance(resp, ExceptionWrapper):
                resp.re_raise()
            else:
                self._scheduler_proc = scheduler_proc

        return scheduler_proc

    def _set_fail_after_n_executions(self, n: int) -> None:
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self._val_fail_after_n_executions = n

    def __hash__(self):
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.tool_version_id)).encode('utf-8'))
        hash_value.update(str(self.workflow_args_hash).encode('utf-8'))
        hash_value.update(str(self.task_hash).encode('utf-8'))
        hash_value.update(str(hash(self._dag)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)
