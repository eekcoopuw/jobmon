"""The overarching framework to create tasks and dependencies within."""
import hashlib
import logging
from multiprocessing import Event, Process, Queue
from multiprocessing import synchronize
from queue import Empty
import time
from typing import Any, Dict, List, Optional, Sequence, Union
import uuid

from jobmon.client.array import Array
from jobmon.client.client_config import ClientConfig
from jobmon.client.client_logging import ClientLogging
from jobmon.client.cluster import Cluster
from jobmon.client.dag import Dag
from jobmon.client.distributor.api import DistributorConfig
from jobmon.client.distributor.distributor_service import (
    DistributorService,
    ExceptionWrapper,
)
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.client.workflow_run import WorkflowRun as ClientWorkflowRun
from jobmon.constants import (
    TaskResourcesType,
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.exceptions import (
    CallableReturnedInvalidObject,
    DistributorNotAlive,
    DistributorStartupTimeout,
    DuplicateNodeArgsError,
    InvalidResponse,
    ResumeSet,
    WorkflowAlreadyComplete,
    WorkflowAlreadyExists,
    WorkflowNotResumable,
)
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeCluster

ClientLogging().attach(__name__)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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

    def __init__(
        self,
        tool_version_id: int,
        workflow_args: str = "",
        name: str = "",
        description: str = "",
        workflow_attributes: Optional[Union[List, dict]] = None,
        max_concurrently_running: int = 10_000,
        requester: Optional[Requester] = None,
        chunk_size: int = 500,  # TODO: should be in the config
    ) -> None:
        """Initialization of the client workflow.

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
        self.arrays: List = []
        self._chunk_size: int = chunk_size

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(uuid.uuid4())
            logger.info(
                "Workflow_args defaulting to uuid {}. To resume this "
                "workflow, you must re-instantiate Workflow and pass "
                "this uuid in as the workflow_args. As a uuid is hard "
                "to remember, we recommend you name your workflows and"
                " make workflow_args a meaningful unique identifier. "
                "Then add the same tasks to this workflow".format(self.workflow_args)
            )
        self.workflow_args_hash = int(
            hashlib.sha1(self.workflow_args.encode("utf-8")).hexdigest(), 16
        )

        self._distributor_com_queue: Queue = Queue()
        self._distributor_stop_event: synchronize.Event = Event()

        self.workflow_attributes: Dict[str, Any] = {}
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
        self.default_cluster_name: str = ""
        self.default_compute_resources_set: Dict[str, Dict[str, Any]] = {}

        self._fail_after_n_executions = 1_000_000_000

    @property
    def is_bound(self) -> bool:
        """If the workflow has been bound to the db."""
        if not hasattr(self, "_workflow_id"):
            return False
        else:
            return True

    @property
    def workflow_id(self) -> int:
        """If the workflow is bound then it will have been given an id."""
        if not self.is_bound:
            raise AttributeError(
                "workflow_id cannot be accessed before workflow is bound"
            )
        return self._workflow_id

    @property
    def dag_id(self) -> int:
        """If it has been bound, it will have an associated dag_id."""
        if not self.is_bound:
            raise AttributeError("dag_id cannot be accessed before workflow is bound")
        return self._dag.dag_id

    @property
    def task_hash(self) -> int:
        """Hash of all of the tasks."""
        hash_value = hashlib.sha1()
        tasks = sorted(self.tasks.values())
        if len(tasks) > 0:  # if there are no tasks, we want to skip this
            for task in tasks:
                hash_value.update(str(hash(task)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    @property
    def task_errors(self) -> Dict:
        """Return a dict of error associated with a task."""
        return {
            task.name: task.get_errors()
            for task in self.tasks.values()
            if task.final_status == TaskStatus.ERROR_FATAL
        }

    def add_attributes(self, workflow_attributes: dict) -> None:
        """Users can call either to update values of existing attributes or add new attributes.

        Args:
            workflow_attributes: attributes to be bound to the db that describe
                this workflow.
        """
        app_route = f"/workflow/{self.workflow_id}/workflow_attributes"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"workflow_attributes": workflow_attributes},
            request_type="put",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )

    def add_task(self, task: Task) -> Task:
        """Add a task to the workflow to be executed.

        Set semantics - add tasks once only, based on hash name. Also creates the job. If
        is_no has no task_id the creates task_id and writes it onto object.

        Args:
            task: single task to add.
        """
        logger.info(f"Adding Task {task}")
        if hash(task) in self.tasks.keys():
            raise ValueError(
                f"A task with hash {hash(task)} already exists. "
                f"All tasks in a workflow must have unique "
                f"commands. Your command was: {task.command}"
            )
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

    def add_tasks(self, tasks: Sequence[Task]) -> None:
        """Add a list of task to the workflow to be executed."""
        for task in tasks:
            # add the task
            self.add_task(task)

    def add_array(self, array: Array) -> None:
        """Add an array and its tasks to the workflow."""
        if len(array.tasks) == 0:
            raise ValueError("Cannot bind an array with no tasks.")
        self.arrays.append(array)
        self.add_tasks(array.tasks)

    def add_arrays(self, arrays: List[Array]) -> None:
        """Add multiple arrays to the workflow."""
        for array in arrays:
            self.add_array(array)

    def bind_arrays(self) -> None:
        """Add the arrays to the database.

        Done sequentially instead of in bulk, since scaling not assumed to be a problem
        with arrays.
        """
        for array in self.arrays:
            cluster = self._get_cluster_by_name(array.default_cluster_name)
            # Create a task resources object and bind to the array
            task_resources = cluster.create_valid_task_resources(
                resource_params=array.default_compute_resources_set,
                task_resources_type_id=TaskResourcesType.VALIDATED,
            )
            task_resources.bind(TaskResourcesType.VALIDATED)
            array.set_task_resources(task_resources)
            array.bind(workflow_id=self.workflow_id, cluster_id=cluster.id)

    def set_default_compute_resources_from_yaml(
        self, cluster_name: str, yaml_file: str
    ) -> None:
        """Set default compute resources from a user provided yaml file for workflow level.

        TODO: Implement this method.

        Args:
            cluster_name: name of cluster to set default values for.
            yaml_file: the yaml file that is providing the compute resource values.
        """
        pass

    def set_default_compute_resources_from_dict(
        self, cluster_name: str, dictionary: Dict[str, Any]
    ) -> None:
        """Set default compute resources for a given cluster_name.

        Args:
            cluster_name: name of cluster to set default values for.
            dictionary: dictionary of default compute resources to run tasks
                with. Can be overridden at task template, tool or task level.
        """
        # TODO: Do we need to handle the scenario where no cluster name is specified?
        self.default_compute_resources_set[cluster_name] = dictionary

    def set_default_cluster_name(self, cluster_name: str) -> None:
        """Set the default cluster.

        Args:
            cluster_name: name of cluster to set as default.
        """
        self.default_cluster_name = cluster_name

    def get_tasks_by_node_args(
        self, task_template_name: str, **kwargs: Any
    ) -> List["Task"]:
        """Query tasks by node args. Used for setting dependencies."""
        tasks: List["Task"] = []
        if self.arrays:
            for array in self.arrays:
                if task_template_name == array.task_template_name:
                    tasks.extend(array.get_tasks_by_node_args(**kwargs))
        return tasks

    def run(
        self,
        fail_fast: bool = False,
        seconds_until_timeout: int = 36000,
        resume: bool = ResumeStatus.DONT_RESUME,
        reset_running_jobs: bool = True,
        distributor_response_wait_timeout: int = 180,
        distributor_config: Optional[DistributorConfig] = None,
        resume_timeout: int = 300,
    ) -> str:
        """Run the workflow.

        Traverse the dag and submitting new tasks when their tasks have completed successfully.

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
            str of WorkflowRunStatus
        """
        # bind to database
        logger.info("Adding Workflow metadata to database")
        self.bind()
        logger.info(f"Workflow ID {self.workflow_id} assigned")

        # create workflow_run
        logger.info("Adding WorkflowRun metadata to database")
        wfr = self._create_workflow_run(resume, reset_running_jobs, resume_timeout)
        logger.info(f"WorkflowRun ID {wfr.workflow_run_id} assigned")

        # start distributor
        self._distributor_proc = self._start_distributor_service(
            wfr.workflow_run_id, distributor_response_wait_timeout, distributor_config
        )

        # set up swarm and initial DAG
        swarm = SwarmWorkflowRun(
            workflow_id=self.workflow_id,
            workflow_run_id=wfr.workflow_run_id,
            tasks=list(self.tasks.values()),
            fail_after_n_executions=self._fail_after_n_executions,
            requester=self.requester,
        )

        try:
            self._run_swarm(swarm, fail_fast, seconds_until_timeout)
        finally:
            # deal with task instance distributor process if it was started
            if self._distributor_alive(raise_error=False):
                logger.info(
                    "Terminating distributing process. This could take a few minutes."
                )
                self._distributor_stop_event.set()
                try:
                    # give it some time to shut down
                    self._distributor_com_queue.get(
                        timeout=distributor_response_wait_timeout
                    )
                except Empty:
                    pass
                self._distributor_proc.terminate()

            # figure out doneness
            num_new_completed = len(swarm.all_done) - swarm.num_previously_complete
            if swarm.status != WorkflowRunStatus.DONE:
                logger.info(
                    f"WorkflowRun execution ended, num failed {len(swarm.all_error)}"
                )
            else:
                logger.info(
                    f"WorkflowRun execute finished successfully, {num_new_completed} tasks"
                )

            # update workflow tasks with final status
            for task in self.tasks.values():
                task.final_status = swarm.swarm_tasks[task.task_id].status
            self._num_previously_completed = swarm.num_previously_complete
            self._num_newly_completed = num_new_completed

        return swarm.status

    def validate(self, fail: bool = False) -> None:
        """Confirm that the tasks in this workflow are valid.

        This method will access the database to confirm the requested resources are valid for
        the specified cluster. It will also confirm that the workflow args are valid.  It also
        will make sure no task contains up/down stream tasks that are not in the workflow.
        """
        # construct task resources
        # TODO: consider moving to create_workflow_run
        for task in self.tasks.values():
            # get the cluster for this task
            cluster_name = self._get_cluster_name(task)
            cluster = self._get_cluster_by_name(cluster_name)

            # add cluster to task
            task.cluster = cluster

            # not dynamic resource request. Construct TaskResources
            if task.compute_resources_callable is None:
                # construct the resource params by traversing from workflow to task
                resource_params = self._get_resource_params(task, cluster_name)

                task.task_resources = cluster.create_valid_task_resources(
                    resource_params, TaskResourcesType.VALIDATED, fail=fail
                )

        # check if workflow is valid
        self._dag.validate()
        self._matching_wf_args_diff_hash()

    def bind(self) -> None:
        """Bind objects to the database if they haven't already been.

        compute_resources: A dictionary that includes the users requested resources
            for the current run. E.g. {cores: 1, mem: 1, runtime: 60, queue: all.q}.
        cluster_name: The specific cluster that the user wants to use.
        """
        if self.is_bound:
            return

        self.validate()
        # bind dag
        self._dag.bind(self._chunk_size)

        # bind workflow
        app_route = "/workflow"
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
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )

        self._workflow_id = response["workflow_id"]
        self._status = response["status"]
        self._newly_created = response["newly_created"]

    def _get_cluster_by_name(self, cluster_name: str) -> Cluster:
        """Check if the cluster that the task specified is in the cache.

        If the cluster is not in the cache, create it and add to cache.
        """
        try:
            cluster = self._clusters[cluster_name]
        except KeyError:
            app_route = f"/cluster/{cluster_name}"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get", logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f"Unexpected status code {return_code} from POST "
                    f"request through route {app_route}. Expected code "
                    f"200. Response content: {response}"
                )
            cluster_kwargs = SerializeCluster.kwargs_from_wire(response["cluster"])
            cluster = Cluster(cluster_name=cluster_kwargs["name"])
            cluster.bind()
            self._clusters[cluster_name] = cluster
        return cluster

    def _get_cluster_name(self, task: Task) -> str:
        cluster_name = task.cluster_name
        if not cluster_name:
            if self.default_cluster_name:
                cluster_name = self.default_cluster_name
            else:
                # TODO: more cluster name validation?
                raise ValueError("No valid cluster_name found on workflow or task")
        return cluster_name

    def _get_resource_params(self, task: Task, cluster_name: str) -> Dict:
        # TODO: once we have concrete task template add ability to get cluster resources
        # from it

        # Check if there are compute resources for given task, if not set at workflow level
        # copy params for idempotent operation
        resource_params = self.default_compute_resources_set.get(
            cluster_name, {}
        ).copy()
        resource_params.update(task.compute_resources.copy())
        return resource_params

    def _create_workflow_run(
        self,
        resume: bool = ResumeStatus.DONT_RESUME,
        reset_running_jobs: bool = True,
        resume_timeout: int = 300,
    ) -> ClientWorkflowRun:
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
            workflow_id=self.workflow_id, requester=self.requester
        )

        # Bind arrays, then tasks
        self.bind_arrays()
        client_wfr.bind(self.tasks, reset_running_jobs, self._chunk_size)
        self._status = WorkflowStatus.QUEUED

        return client_wfr

    def _run_swarm(
        self,
        swarm: SwarmWorkflowRun,
        fail_fast: bool = False,
        seconds_until_timeout: int = 36000,
        wedged_workflow_sync_interval: int = 600,
    ) -> SwarmWorkflowRun:
        """Take a concrete DAG and queue al the Tasks that are not DONE.

        Uses forward chaining from initial fringe, hence out-of-date is not
        applied transitively backwards through the graph. It could also use
        backward chaining from an identified goal node, the effect is
        identical.

        The internal data structures are lists, but might need to be changed
        to be better at scaling.

        Conceptually:
        Put all tasks w/ finished upstreams on the ready_to_run queue
        Put tasks in Adjusting state on the ready_to_run queue

        while there are tasks ready_to_run or currently running tasks:
            queue all tasks that are ready_to_run
            wait for some jobs to complete and add downstreams to the ready_to_run queue
            rinse and repeat

        Args:
            swarm: the workflow run associated with the swarm.
            fail_fast: raise error on the first failed task.
            seconds_until_timeout: how long to block while waiting for the next task to finish
                before raising an error.
            wedged_workflow_sync_interval: the time interval to sync a workflow that is wedged.

        Return:
            workflow_run status
        """
        try:
            logger.info(f"Executing Workflow Run {swarm.workflow_run_id}")
            swarm.update_status(WorkflowRunStatus.RUNNING)
            swarm.compute_initial_dag_state()
        except Exception:
            swarm.update_status(WorkflowRunStatus.ERROR)
            raise

        # These are all Tasks.
        # While there is something ready to be run, or something is running
        while swarm.ready_to_run or swarm.active_tasks:

            # queue any ready to run and then wait until active tasks change state
            try:

                for swarm_task in swarm.queue_tasks():
                    task = self.tasks[swarm_task.task_hash]
                    task_resources = self._get_dyamic_task_resources(task)
                    task_resources.bind()
                    swarm_task.task_resources = task_resources

                # wait till we have new work
                swarm.block_until_newly_ready_or_all_done(
                    fail_fast,
                    seconds_until_timeout=seconds_until_timeout,
                    wedged_workflow_sync_interval=wedged_workflow_sync_interval,
                    distributor_alive_callable=self._distributor_alive,
                )
            # user interrupt
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    logger.warning(
                        "Keyboard interrupt raised and Workflow Run set to Stopped"
                    )
                    swarm.update_status(WorkflowRunStatus.STOPPED)
                    raise
                else:
                    logger.info("Continuing jobmon...")

            # scheduler died
            except DistributorNotAlive:
                # check if we got an exception from the distributor
                try:
                    resp = self._distributor_com_queue.get(False)

                # no response. set error state and re-raise DistributorNotAlive
                except Empty:
                    swarm.update_status(WorkflowRunStatus.ERROR)
                    raise

                # got response. process
                else:
                    # if response is an exception re-raise error from distributor
                    if isinstance(resp, ExceptionWrapper):
                        try:
                            resp.re_raise()

                        # if it was a resume exception we set terminate state
                        except ResumeSet:
                            swarm.terminate_workflow_run()
                            raise

                        # otherwise set to error state
                        except Exception:
                            swarm.update_status(WorkflowRunStatus.ERROR)
                            raise
                    else:
                        # response is not an exception
                        swarm.update_status(WorkflowRunStatus.ERROR)
                        raise

            except Exception:
                swarm.update_status(WorkflowRunStatus.ERROR)
                raise

        # END while fringe or all_active

        # To be a dynamic-DAG tool, we must be prepared for the DAG to have
        # changed. In general we would recompute forward from the fringe.
        # Not efficient, but correct. A more efficient algorithm would be to
        # check the nodes that were added to see if they should be in the
        # fringe, or if they have potentially affected the status of Tasks
        # that were done (error case - disallowed??)

        # update status based on tasks if no error
        if swarm.all_error:
            swarm.update_status(WorkflowRunStatus.ERROR)
        else:
            swarm.update_status(WorkflowRunStatus.DONE)

        return swarm

    def _matching_wf_args_diff_hash(self) -> None:
        """Check that that an existing workflow does not contain different tasks.

        Check that an existing workflow with the same workflow_args does not have a
        different hash, this would indicate that thgat the workflow contains different tasks.
        """
        rc, response = self.requester.send_request(
            app_route=f"/workflow/{str(self.workflow_args_hash)}",
            message={},
            request_type="get",
            logger=logger,
        )
        bound_workflow_hashes = response["matching_workflows"]
        for task_hash, tool_version_id, dag_hash in bound_workflow_hashes:
            match = self.tool_version_id == tool_version_id and (
                str(self.task_hash) != task_hash or str(hash(self._dag)) != dag_hash
            )
            if match:
                raise WorkflowAlreadyExists(
                    "The unique workflow_args already belong to a workflow "
                    "that contains different tasks than the workflow you are "
                    "creating, either change your workflow args so that they "
                    "are unique for this set of tasks, or make sure your tasks"
                    " match the workflow you are trying to resume"
                )

    def _set_workflow_resume(self, reset_running_jobs: bool = True) -> None:
        app_route = f"/workflow/{self.workflow_id}/set_resume"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "reset_running_jobs": reset_running_jobs,
                "description": self.description,
                "name": self.name,
                "max_concurrently_running": self.max_concurrently_running,
                "workflow_attributes": self.workflow_attributes,
            },
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

    def _workflow_is_resumable(self, resume_timeout: int = 300) -> None:
        # previous workflow exists but is resumable. we will wait till it terminates
        wait_start = time.time()
        workflow_is_resumable = False
        while not workflow_is_resumable:
            logger.info(
                f"Waiting for resume. "
                f"Timeout in {round(resume_timeout - (time.time() - wait_start), 1)}"
            )
            app_route = f"/workflow/{self.workflow_id}/is_resumable"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get", logger=logger
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f"Unexpected status code {return_code} from POST "
                    f"request through route {app_route}. Expected "
                    f"code 200. Response content: {response}"
                )

            workflow_is_resumable = response.get("workflow_is_resumable")
            if (time.time() - wait_start) > resume_timeout:
                raise WorkflowNotResumable(
                    "workflow_run timed out waiting for previous "
                    "workflow_run to exit. Try again in a few minutes."
                )
            else:
                sleep_time = round(float(resume_timeout) / 10.0, 1)
                time.sleep(sleep_time)

    def _start_distributor_service(
        self,
        workflow_run_id: int,
        distributor_startup_wait_timeout: int = 180,
        distributor_config: Optional[DistributorConfig] = None,
    ) -> Process:
        if distributor_config is None:
            distributor_config = DistributorConfig.from_defaults()

        cluster_names = list(self._clusters.keys())
        if len(cluster_names) > 1:
            raise RuntimeError(
                f"Workflow can only use one cluster. Found cluster_names={cluster_names}"
            )
        else:
            cluster_plugin = self._clusters[cluster_names[0]].plugin
            DistributorCls = cluster_plugin.get_cluster_distributor_class()
            distributor = DistributorCls()

        logger.info("Instantiating Distributor Process")

        # instantiate DistributorService and launch in separate proc. use event to
        # signal back when distributor is started
        ti_hi = distributor_config.task_instance_heartbeat_interval
        tid = DistributorService(
            workflow_id=self.workflow_id,
            workflow_run_id=workflow_run_id,
            distributor=distributor,
            workflow_run_heartbeat_interval=distributor_config.workflow_run_heartbeat_interval,
            task_instance_heartbeat_interval=ti_hi,
            heartbeat_report_by_buffer=distributor_config.heartbeat_report_by_buffer,
            n_queued=distributor_config.n_queued,
            distributor_poll_interval=distributor_config.distributor_poll_interval,
            requester=self.requester,
            wf_max_concurrently_running=self.max_concurrently_running
        )
        self._status = WorkflowStatus.INSTANTIATING

        distributor_proc = Process(
            target=tid.run_distributor,
            args=(self._distributor_stop_event, self._distributor_com_queue),
        )

        try:
            # Start the distributor
            distributor_proc.start()

            # wait for response from distributor
            resp = self._distributor_com_queue.get(
                timeout=distributor_startup_wait_timeout
            )
        except Empty:  # mypy complains but this is correct
            distributor_proc.terminate()
            raise DistributorStartupTimeout(
                "Distributor process did not start within the alloted timeout "
                f"t={distributor_startup_wait_timeout}s"
            )
        else:
            # the first message can only be "ALIVE" or an ExceptionWrapper
            if isinstance(resp, ExceptionWrapper):
                resp.re_raise()

        return distributor_proc

    def _distributor_alive(self, raise_error: bool = True) -> bool:
        """If the distributor process is still active."""
        if not hasattr(self, "_distributor_proc"):
            alive = False
        else:
            logger.debug(f"Distributor proc is: {self._distributor_proc.is_alive()}")
            alive = self._distributor_proc.is_alive()

        if raise_error and not alive:
            raise DistributorNotAlive(
                f"Distributor process pid=({self._distributor_proc.pid}) unexpectedly died"
                f" with exit code {self._distributor_proc.exitcode}"
            )
        return alive

    def _get_dyamic_task_resources(self, task: Task) -> TaskResources:
        # this can't be none but put a check in any case
        func = task.compute_resources_callable
        if func is None:
            raise RuntimeError(
                "Internal error. Jobmon tried to compute dynamic task resources,"
                " but no callable was available. Please contact the maintainers "
                "of jobmon for support."
            )

        # compute dynamic resources
        dynamic_compute_resources = func()
        if not isinstance(dynamic_compute_resources, dict):
            raise CallableReturnedInvalidObject(
                f"compute_resources_callable={func} for task_id={task.task_id} "
                "returned an invalid type. Must return dict. got "
                f"{type(dynamic_compute_resources)}."
            )
        resource_params = self._get_resource_params(task, task.cluster.cluster_name)
        resource_params.update(dynamic_compute_resources)
        task_resources = task.cluster.create_valid_task_resources(
            resource_params, TaskResourcesType.VALIDATED
        )
        return task_resources

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self.tool_version_id)).encode("utf-8"))
        hash_value.update(str(self.workflow_args_hash).encode("utf-8"))
        hash_value.update(str(self.task_hash).encode("utf-8"))
        hash_value.update(str(hash(self._dag)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    def get_errors(
        self, limit: int = 1000
    ) -> Optional[Dict[int, Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]]]:
        """Method to get all errors.

        Return a dictionary with the erring task_id as the key, and
        the Task.get_errors content as the value.
        When limit is specifically set as None from the client, this
        return set will pass back all the erred tasks in the workflow.
        """
        errors = {}

        cnt: int = 0
        for task in self.tasks.values():
            task_id = task.task_id
            task_errors = task.get_errors()
            if task_errors is not None and len(task_errors) > 0:
                errors[task_id] = task_errors
                cnt += 1
                if limit is not None and cnt >= limit - 1:
                    break

        return errors
