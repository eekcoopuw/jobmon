"""The overarching framework to create tasks and dependencies within."""
import hashlib
import logging
import time
import uuid
import warnings
from multiprocessing import Event, Process, Queue
from multiprocessing import synchronize
from queue import Empty
from typing import Any, Dict, List, Optional, Sequence, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.cluster import Cluster
from jobmon.client.dag import Dag
from jobmon.client.distributor.api import DistributorConfig
from jobmon.client.distributor.distributor_service import \
    ExceptionWrapper, DistributorService
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.task import Task
from jobmon.client.workflow_run import WorkflowRun as ClientWorkflowRun
from jobmon.cluster_type.api import import_cluster
from jobmon.constants import WorkflowRunStatus, WorkflowStatus
from jobmon.exceptions import (DuplicateNodeArgsError, InvalidResponse, ResumeSet,
                               DistributorNotAlive, DistributorStartupTimeout,
                               WorkflowAlreadyComplete, WorkflowAlreadyExists,
                               WorkflowNotResumable)
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeCluster


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

    def __init__(self,
                 tool_version_id: int,
                 workflow_args: str = "",
                 name: str = "",
                 description: str = "",
                 workflow_attributes: Optional[Union[List, dict]] = None,
                 max_concurrently_running: int = 10_000,
                 requester: Optional[Requester] = None,
                 chunk_size: int = 500,  # TODO: should be in the config
                 ):
        """
        Args:
            tool_version_id: id of the associated tool
            workflow_args: Unique identifier of a workflow
            name: Name of the workflow
            description: Description of the workflow
            workflow_attributes: Attributes that make this workflow different from other
                workflows that the user wants to record.
            max_concurrently_running: How many running jobs to allow in parallel
            requester: object to communicate with the flask services.
            chunk_size: how many tasks to bind in a single request
        """
        self.tool_version_id = tool_version_id
        self.name = name
        self.description = description
        self.max_concurrently_running = max_concurrently_running

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        self._dag = Dag(requester)
        # hash to task object mapping. ensure only 1
        self.tasks: Dict[int, Task] = {}
        self._swarm_tasks: dict = {}
        self._chunk_size: int = chunk_size

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

        self._distributor_proc: Optional[Process] = None
        self._distributor_com_queue: Queue = Queue()
        self._distributor_stop_event: synchronize.Event = Event()
        self.workflow_attributes = {}
        if workflow_attributes:
            if isinstance(workflow_attributes, List):
                for attr in workflow_attributes:
                    self.workflow_attributes[attr] = None
            elif isinstance(workflow_attributes, dict):
                for attr, val in workflow_attributes.items():
                    self.workflow_attributes[str(attr)] = str(val)
            else:
                raise ValueError(
                    "workflow_attributes must be provided as a list of attributes or a "
                    "dictionary of attributes and their values"
                )

        # Cache for clusters
        self._clusters: Dict[str, Cluster] = {}

    @property
    def is_bound(self):
        """If the workflow has been bound to the db."""
        if not hasattr(self, "_workflow_id"):
            return False
        else:
            return True

    @property
    def workflow_id(self) -> int:
        """If the workflow is bound then it will have been given an id."""
        if not self.is_bound:
            raise AttributeError("workflow_id cannot be accessed before workflow is bound")
        return self._workflow_id

    @property
    def dag_id(self) -> int:
        """If it has been bound, it will have an associated dag_id."""
        if not self.is_bound:
            raise AttributeError("dag_id cannot be accessed before workflow is bound")
        return self._dag.dag_id

    @property
    def task_hash(self):
        """Hash of all of the tasks."""
        hash_value = hashlib.sha1()
        tasks = sorted(self.tasks.values())
        if len(tasks) > 0:  # if there are no tasks, we want to skip this
            for task in tasks:
                hash_value.update(str(hash(task)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)

    def add_attributes(self, workflow_attributes: dict) -> None:
        """Function that users can call either to update values of existing attributes or add
        new attributes.

        Args:
            workflow_attributes: attributes to be bound to the db that describe
                this workflow.
        """
        app_route = f'/client/workflow/{self.workflow_id}/workflow_attributes'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"workflow_attributes": workflow_attributes},
            request_type="put",
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}')

    def add_task(self, task: Task) -> Task:
        """Add a task to the workflow to be executed. Set semantics - add tasks once only,
        based on hash name. Also creates the job. If is_no has no task_id the creates task_id
        and writes it onto object.

        Args:
            task: single task to add
        """
        logger.info(f"Adding Task {task}")
        if hash(task) in self.tasks.keys():
            raise ValueError(f"A task with hash {hash(task)} already exists. "
                             f"All tasks in a workflow must have unique "
                             f"commands. Your command was: {task.command}")
        try:
            self._dag.add_node(task.node)
        except DuplicateNodeArgsError:
            raise DuplicateNodeArgsError(
                "All tasks for a given task template in a workflow must have unique node_args."
                f"Found duplicate node args for {task}. task_template_version_id="
                f"{task.node.task_template_version_id}, node_args={task.node.node_args}"
            )
        self.tasks[hash(task)] = task

        logger.debug(f"Task {hash(task)} added")

        return task

    def add_tasks(self, tasks: Sequence[Task]):
        """Add a list of task to the workflow to be executed."""
        for task in tasks:
            # add the task
            self.add_task(task)

    def set_default_cluster(self, cluster_name: Optional[str] = 'buster'):
        # TODO: optionally pass to tasks unless specified at a lower level
        pass

    def set_default_compute_resources(self):
        pass

    def _get_cluster_by_name(self, cluster_name: str) -> Cluster:
        """Check if the cluster that the task specified is in the cache, if not create it and
        add to cache"""
        try:
            cluster = self._clusters[cluster_name]
        except KeyError:
            app_route = f'/client/cluster/{cluster_name}'
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={},
                request_type="get",
                logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected code '
                    f'200. Response content: {response}'
                )
            cluster_kwargs = SerializeCluster.kwargs_from_wire(response["cluster"])
            cluster = Cluster(cluster_name=cluster_kwargs["name"])
            cluster.bind()
            self._clusters[cluster_name] = cluster
        return cluster

    def run(self, fail_fast: bool = False, seconds_until_timeout: int = 36000,
            resume: bool = ResumeStatus.DONT_RESUME, reset_running_jobs: bool = True,
            distributor_response_wait_timeout: int = 180,
            distributor_config: Optional[DistributorConfig] = None,
            resume_timeout: int = 300) -> WorkflowRun:
        """Run the workflow by traversing the dag and submitting new tasks when their tasks
        have completed successfully.

        Args:
            fail_fast: whether or not to break out of distributor on
                first failure
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not set to resume and an identical workflow already
                exists, the workflow will error out
            reset_running_jobs: whether or not to reset running jobs upon resume
            distributor_response_wait_timeout: amount of time to wait for the
                distributor thread to start up
            distributor_config: a distributor config object
            resume_timeout: seconds to wait for a workflow to become resumable before giving up

        Returns:
            object of WorkflowRun, can be checked to make sure all jobs ran to completion,
                checked for status, etc.
        """
        warnings.warn(
            "From Jobmon 3.0 on, the return type of Workflow.run will no longer be "
            "swarm/WorkflowRun. It will be WorkflowRunStatus instead. Please plan "
            "accordingly.",
            PendingDeprecationWarning
        )
        if not hasattr(self, "_distributor"):
            logger.debug("using default project: ihme_general")
            self.set_distributor(project="ihme_general")
        logger.debug("distributor: {}".format(self._distributor))

        # bind to database
        logger.info("Adding Workflow metadata to database")
        self.bind()
        logger.info(f"Workflow ID {self.workflow_id} assigned")

        # create workflow_run
        logger.info("Adding WorkflowRun metadata to database")
        wfr = self._create_workflow_run(resume, reset_running_jobs, resume_timeout)
        logger.info(f"WorkflowRun ID {wfr.workflow_run_id} assigned")

        # testing parameter
        if hasattr(self, "_val_fail_after_n_executions"):
            wfr._set_fail_after_n_executions(self._val_fail_after_n_executions)

        try:
            # start distributor
            distributor_proc = self._start_distributor_service(
                wfr.workflow_run_id, distributor_response_wait_timeout, distributor_config
            )
            # execute the workflow run
            wfr.execute_interruptible(distributor_proc, fail_fast, seconds_until_timeout)
            logger.info(f"WorkflowRun run finished executing. Status is: {wfr.status}")
            return wfr

        except KeyboardInterrupt:
            wfr.update_status(WorkflowRunStatus.STOPPED)
            logger.warning("Keyboard interrupt raised and Workflow Run set to Stopped")
            return wfr

        except DistributorNotAlive:
            # check if we got an exception from the distributor
            try:
                resp = self._distributor_com_queue.get(False)
            except Empty:
                wfr.update_status(WorkflowRunStatus.ERROR)
                # no response. raise distributor not alive
                raise
            else:
                # re-raise error from distributor
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
                distributor_proc.terminate()
                self._distributor_proc = None
                return wfr

        except Exception:
            wfr.update_status(WorkflowRunStatus.ERROR)
            raise

        finally:
            # deal with task instance distributor process if it was started
            logger.info("Terminating distributing process. This could take a few minutes.")
            if self._distributor_proc is not None:
                self._distributor_stop_event.set()
                try:
                    # give it some time to shut down
                    self._distributor_com_queue.get(timeout=distributor_response_wait_timeout)
                except Empty:
                    pass
                self._distributor_proc.terminate()

    def bind(self, cluster_name: Optional[str] = None, compute_resources: Optional[Dict] = None
             ):
        """Bind objects to the database if they haven't already been"""
        if self.is_bound:
            return

        for task in self.tasks.values():
            if task.cluster_name is None:
                if cluster_name is None:
                    # TODO: more cluster name validation
                    raise ValueError("No valid cluster_name found on task or workflow.")
                else:
                    task.cluster_name = cluster_name

            # TODO figure out user API rule, for how to handle a user passing in partial dicts
            # to task
            # Check if there are compute resources for given task, if not set at workflow level
            if task.compute_resources is None:
                if compute_resources is None:
                    raise ValueError("No compute_resources found on task or workflow.")
                else:
                    task.compute_resources = compute_resources.copy()
            else:
                if compute_resources is not None:
                    task.compute_resources = {**compute_resources, **task.compute_resources}

            cluster = self._get_cluster_by_name(task.cluster_name)
            task_resources = cluster.create_task_resources(task.compute_resources)
            task.task_resources = task_resources

        # 1) can only have 1 cluster
        # 2) workflow cluster is overridden by the task cluster

        # check if workflow is valid
        self._dag.validate()  # TODO: this does nothing at the moment
        self._matching_wf_args_diff_hash()

        # bind dag
        self._dag.bind(self._chunk_size)

        # bind workflow
        app_route = '/client/workflow'
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
                "max_concurrently_running": self.max_concurrently_running,
                "workflow_attributes": self.workflow_attributes,
            },
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        self._workflow_id = response["workflow_id"]
        self._status = response["status"]
        self._newly_created = response["newly_created"]

    def _create_workflow_run(self, resume: bool = ResumeStatus.DONT_RESUME,
                             reset_running_jobs: bool = True,
                             resume_timeout: int = 300) -> WorkflowRun:
        # raise error if workflow exists and is done
        if self._status == WorkflowStatus.DONE:
            raise WorkflowAlreadyComplete(
                f"Workflow ({self.workflow_id}) is already in done state and cannot be resumed"
            )

        if not self._newly_created and not resume:
            raise WorkflowAlreadyExists(
                "This workflow already exists. If you are trying to resume a workflow, "
                "please set the resume flag. If you are not trying to resume a workflow, make "
                "sure the workflow args are unique or the tasks are unique"
            )
        elif not self._newly_created and resume:
            self._set_workflow_resume(reset_running_jobs)
            self._workflow_is_resumable(resume_timeout)

        # create workflow run
        client_wfr = ClientWorkflowRun(
            workflow_id=self.workflow_id,
            requester=self.requester
        )
        client_wfr.bind(self.tasks, reset_running_jobs, self._chunk_size)
        self._status = WorkflowStatus.QUEUED

        # create swarm workflow run
        swarm_tasks: Dict[int, SwarmTask] = {}
        for task in self.tasks.values():

            # create swarmtasks
            swarm_task = SwarmTask(
                task_id=task.task_id,
                status=task.initial_status,
                task_args_hash=task.task_args_hash,
                task_resources=task.task_resources,
                max_attempts=task.max_attempts
            )
            swarm_tasks[task.task_id] = swarm_task

        # create relationships on swarm tasks
        for task in self.tasks.values():
            swarm_task = swarm_tasks[task.task_id]
            swarm_task.upstream_swarm_tasks = set([
                swarm_tasks[t.task_id] for t in task.upstream_tasks])
            swarm_task.downstream_swarm_tasks = set([
                swarm_tasks[t.task_id] for t in task.downstream_tasks])

        wfr = WorkflowRun(
            workflow_id=client_wfr.workflow_id,
            workflow_run_id=client_wfr.workflow_run_id,
            swarm_tasks=swarm_tasks,
            requester=self.requester
        )

        return wfr

    def _matching_wf_args_diff_hash(self):
        """Check that an existing workflow with the same workflow_args does not have a
        different hash indicating that it contains different tasks.
        """
        rc, response = self.requester.send_request(
            app_route=f'/client/workflow/{str(self.workflow_args_hash)}',
            message={},
            request_type='get',
            logger=logger
        )
        bound_workflow_hashes = response['matching_workflows']
        for task_hash, tool_version_id, dag_hash in bound_workflow_hashes:
            match = (self.task_hash == task_hash and self.tool_version_id and hash(
                self.dag) == dag_hash)
            if match:
                raise WorkflowAlreadyExists(
                    "The unique workflow_args already belong to a workflow "
                    "that contains different tasks than the workflow you are "
                    "creating, either change your workflow args so that they "
                    "are unique for this set of tasks, or make sure your tasks"
                    " match the workflow you are trying to resume")

    def _set_workflow_resume(self, reset_running_jobs: bool = True):
        app_route = f'/client/workflow/{self.workflow_id}/set_resume'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                'reset_running_jobs': reset_running_jobs,
                'description': self.description,
                'name': self.name,
                'max_concurrently_running': self.max_concurrently_running,
                'workflow_attributes': self.workflow_attributes,
            },
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def _workflow_is_resumable(self, resume_timeout: int = 300):
        # previous workflow exists but is resumable. we will wait till it terminates
        wait_start = time.time()
        workflow_is_resumable = False
        while not workflow_is_resumable:
            logger.info(f"Waiting for resume. "
                        f"Timeout in {round(resume_timeout - (time.time() - wait_start), 1)}")
            app_route = f'/client/workflow/{self.workflow_id}/is_resumable'
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message={},
                request_type='get',
                logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from POST '
                    f'request through route {app_route}. Expected '
                    f'code 200. Response content: {response}')

            workflow_is_resumable = response.get("workflow_is_resumable")
            if (time.time() - wait_start) > resume_timeout:
                raise WorkflowNotResumable("workflow_run timed out waiting for previous "
                                           "workflow_run to exit. Try again in a few minutes.")
            else:
                sleep_time = round(float(resume_timeout) / 10., 1)
                time.sleep(sleep_time)

    def _start_distributor_service(self, workflow_run_id: int,
                                   distributor_startup_wait_timeout: int,
                                   cluster_type_name: str,
                                   distributor_config: Optional[DistributorConfig] = None
                                   ) -> Process:
        if distributor_config is None:
            distributor_config = DistributorConfig.from_defaults()

        module = import_cluster(cluster_type_name)
        ClusterDistributor = module.get_cluster_distributor_class()
        self._distributor = ClusterDistributor()

        logger.info("Instantiating Distributor Process")

        # instantiate DistributorService and launch in separate proc. use event to
        # signal back when distributor is started
        tid = DistributorService(
            workflow_id=self.workflow_id,
            workflow_run_id=workflow_run_id,
            distributor=self._distributor,
            workflow_run_heartbeat_interval=distributor_config.workflow_run_heartbeat_interval,
            task_heartbeat_interval=distributor_config.task_heartbeat_interval,
            heartbeat_report_by_buffer=distributor_config.heartbeat_report_by_buffer,
            n_queued=distributor_config.n_queued,
            distributor_poll_interval=distributor_config.distributor_poll_interval,
            jobmon_command=distributor_config.jobmon_command,
            requester=self.requester
        )
        self._status = WorkflowStatus.INSTANTIATING

        distributor_proc = Process(
            target=tid.run_distributor,
            args=(self._distributor_stop_event, self._distributor_com_queue)
        )

        try:

            # Start the distributor
            distributor_proc.start()

            # wait for response from distributor
            resp = self._distributor_com_queue.get(timeout=distributor_startup_wait_timeout)
        except Empty:  # mypy complains but this is correct
            raise DistributorStartupTimeout("Distributor process did not start within the alloted "
                                          f"timeout t={distributor_startup_wait_timeout}s")
        else:
            # the first message can only be "ALIVE" or an ExceptionWrapper
            if isinstance(resp, ExceptionWrapper):
                resp.re_raise()
            else:
                self._distributor_proc = distributor_proc

        return distributor_proc

    def _set_fail_after_n_executions(self, n: int) -> None:
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self._val_fail_after_n_executions = n

    def __hash__(self):
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.tool_version_id)).encode('utf-8'))
        hash_value.update(str(self.workflow_args_hash).encode('utf-8'))
        hash_value.update(str(self.task_hash).encode('utf-8'))
        hash_value.update(str(hash(self._dag)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)
