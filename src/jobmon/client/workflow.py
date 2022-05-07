"""The overarching framework to create tasks and dependencies within."""
import hashlib
import logging
import logging.config
import psutil
from subprocess import Popen, PIPE, TimeoutExpired
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Union
import uuid

from jobmon.client.array import Array
from jobmon.client.client_config import ClientConfig
from jobmon.client.logging import JobmonLoggerConfig
from jobmon.cluster import Cluster
from jobmon.client.dag import Dag
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.task import Task
from jobmon.client.tool_version import ToolVersion
from jobmon.client.workflow_run import WorkflowRun as ClientWorkflowRun
from jobmon.constants import (
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.exceptions import (
    DistributorStartupTimeout,
    DuplicateNodeArgsError,
    InvalidResponse,
    WorkflowAlreadyComplete,
    WorkflowAlreadyExists,
    WorkflowNotResumable,
)
from jobmon.requester import http_request_ok, Requester


logger = logging.getLogger(__name__)


class DistributorContext:
    def __init__(self, cluster_name: str, workflow_run_id: int, timeout: int):
        self._cluster_name = cluster_name
        self._workflow_run_id = workflow_run_id
        self._timeout = timeout

    def __enter__(self):
        logger.info("Starting Distributor Process")

        # Start the distributor. Write stderr to a file.
        cmd = [
            sys.executable,
            "-m",  # safest way to find the entrypoint
            "jobmon.client.distributor.cli",
            "start",
            "--cluster_name",
            self._cluster_name,
            "--workflow_run_id",
            str(self._workflow_run_id),
        ]
        self.process = Popen(cmd, stderr=PIPE, universal_newlines=True)

        # check if stderr contains "ALIVE"
        assert self.process.stderr is not None  # keep mypy happy on optional type
        stderr_val = self.process.stderr.read(5)
        if stderr_val != "ALIVE":
            err = self._shutdown()
            raise DistributorStartupTimeout(
                f"Distributor process did not start, stderr='{err}'"
            )
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        logger.info("Stopping Distributor Process")
        err = self._shutdown()
        logger.info(f"Got {err} from Distributor Process")

    def alive(self) -> bool:
        self.process.poll()
        return self.process.returncode is None

    def _shutdown(self) -> str:
        self.process.terminate()
        try:
            _, err = self.process.communicate(timeout=self._timeout)
        except TimeoutExpired:
            err = ""

        if "SHUTDOWN" not in err:
            try:
                parent = psutil.Process(self.process.pid)
                for child in parent.children(recursive=True):
                    child.kill()
            except psutil.NoSuchProcess:
                pass
            self.process.kill()
            self.process.wait()

        return err


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
        tool_version: ToolVersion,
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
            tool_version: ToolVersion this workflow is associated
            workflow_args: Unique identifier of a workflow
            name: Name of the workflow
            description: Description of the workflow
            workflow_attributes: Attributes that make this workflow different from other
                workflows that the user wants to record.
            max_concurrently_running: How many running jobs to allow in parallel
            requester: object to communicate with the flask services.
            chunk_size: how many tasks to bind in a single request
        """
        self._tool_version = tool_version
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
        self.arrays: Dict[str, Array] = {}
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
        self.default_resource_scales_set: Dict[str, Dict[str, float]] = {}

        self._fail_after_n_executions = 1_000_000_000
        self.last_workflow_run_id = None

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
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )

    def add_task(self, task: Task) -> Task:
        """Add a task to the workflow to be executed.

        Set semantics - add tasks once only, based on hash name.

        Args:
            task: single task to add.
        """
        logger.debug(f"Adding Task {task}")
        if hash(task) in self.tasks.keys():
            raise ValueError(
                f"A task with hash {hash(task)} already exists. "
                f"All tasks in a workflow must have unique "
                f"commands. Your command was: {task.command}"
            )

        # infer array if not already assigned
        try:
            task.array
        except AttributeError:
            template_name = task.node.task_template_version.task_template.template_name
            try:
                array = self.arrays[template_name]
            except KeyError:
                # create array from the task template version on the node
                array = Array(
                    task_template_version=task.node.task_template_version,
                    task_args=task.task_args,
                    op_args=task.op_args,
                    cluster_name=task.cluster_name,
                )
                self._link_array_and_workflow(array)

            # add task to inferred array
            array.add_task(task)

        # add node to task
        try:
            self._dag.add_node(task.node)
        except DuplicateNodeArgsError:
            raise DuplicateNodeArgsError(
                "All tasks for a given task template in a workflow must have unique node_args."
                f"Found duplicate node args for {task}. task_template_version_id="
                f"{task.node.task_template_version_id}, node_args={task.node.node_args}"
            )

        # add task to workflow
        self.tasks[hash(task)] = task
        task.workflow = self

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
        self._link_array_and_workflow(array)
        self.add_tasks(list(array.tasks.values()))

    def add_arrays(self, arrays: List[Array]) -> None:
        """Add multiple arrays to the workflow."""
        for array in arrays:
            self.add_array(array)

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

    def set_default_resource_scales_from_dict(
        self, cluster_name: str, dictionary: Dict[str, float]
    ) -> None:
        """Set default resource scales for a given cluster_name.

        Args:
            cluster_name: name of cluster to set default values for.
            dictionary: dictionary of default resource scales to adjust task
                resources with. Can be overridden at task template or task level.
        """
        # TODO: Do we need to handle the scenario where no cluster name is specified?
        self.default_resource_scales_set[cluster_name] = dictionary

    def set_default_cluster_name(self, cluster_name: str) -> None:
        """Set the default cluster.

        Args:
            cluster_name: name of cluster to set as default.
        """
        self.default_cluster_name = cluster_name

    def get_tasks_by_node_args(
        self, task_template_name: str, **kwargs: Any
    ) -> List[Task]:
        """Query tasks by node args. Used for setting dependencies."""
        try:
            array = self.arrays[task_template_name]
        except KeyError:
            raise ValueError(
                f"task_template_name={task_template_name} not found on workflow. Known "
                f"template_names are {self.arrays.keys()}."
            )
        tasks = array.get_tasks_by_node_args(**kwargs)
        return tasks

    def run(
        self,
        fail_fast: bool = False,
        seconds_until_timeout: int = 36000,
        resume: bool = False,
        reset_running_jobs: bool = True,
        distributor_startup_timeout: int = 180,
        resume_timeout: int = 300,
        configure_logging: bool = False,
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
            distributor_startup_timeout: amount of time to wait for the distributor process to
                start up
            resume_timeout: seconds to wait for a workflow to become resumable before giving up
            configure_logging: setup jobmon logging. If False, no logging will be configured.
                If True, default logging will be configured.

        Returns:
            str of WorkflowRunStatus
        """
        if configure_logging is True:
            JobmonLoggerConfig.attach_default_handler(
                logger_name="jobmon", log_level=logging.INFO
            )

        # bind to database
        logger.info("Adding Workflow metadata to database")
        self.bind()
        logger.info(f"Workflow ID {self.workflow_id} assigned")

        # create workflow_run
        logger.info("Adding WorkflowRun metadata to database")
        wfr = self._create_workflow_run(resume, reset_running_jobs, resume_timeout)
        logger.info(f"WorkflowRun ID {wfr.workflow_run_id} assigned")

        # start distributor
        cluster_name = list(self._clusters.keys())[0]
        with DistributorContext(
            cluster_name, wfr.workflow_run_id, distributor_startup_timeout
        ) as distributor:

            # set up swarm and initial DAG
            swarm = SwarmWorkflowRun(
                workflow_run_id=wfr.workflow_run_id,
                fail_after_n_executions=self._fail_after_n_executions,
                requester=self.requester,
                fail_fast=fail_fast,
            )
            swarm.from_workflow(self)
            self._num_previously_completed = swarm.num_previously_complete

            try:
                swarm.run(distributor.alive, seconds_until_timeout)
            finally:
                # figure out doneness
                num_new_completed = (
                    len(swarm.done_tasks) - swarm.num_previously_complete
                )
                if swarm.status != WorkflowRunStatus.DONE:
                    logger.info(
                        f"WorkflowRun execution ended, num failed {len(swarm.failed_tasks)}"
                    )
                else:
                    logger.info(
                        f"WorkflowRun execute finished successfully, {num_new_completed} tasks"
                    )

                # update workflow tasks with final status
                for task in self.tasks.values():
                    task.final_status = swarm.tasks[task.task_id].status
                self._num_newly_completed = num_new_completed

        self.last_workflow_run_id = wfr.workflow_run_id

        return swarm.status

    def validate(self, fail: bool = True) -> None:
        """Confirm that the tasks in this workflow are valid.

        This method will access the database to confirm the requested resources are valid for
        the specified cluster. It will also confirm that the workflow args are valid.  It also
        will make sure no task contains up/down stream tasks that are not in the workflow.
        """
        # construct task resources
        for task in self.tasks.values():
            # get the cluster for this task
            cluster = self.get_cluster_by_name(task.cluster_name)

            # not dynamic resource request. Construct TaskResources
            if task.compute_resources_callable is None:
                resource_params = task.compute_resources
                try:
                    queue_name: str = resource_params["queue"]
                except KeyError:
                    queue_msg = (
                        "A queue name must be provided in the specified compute resources. Got"
                        f" compute_resources={resource_params}"
                    )
                    if fail:
                        raise ValueError(queue_msg)
                    else:
                        logger.info(queue_msg)
                        continue

                # validate the constructed resources
                queue = cluster.get_queue(queue_name)
                is_valid, msg, valid_resources = queue.validate_resources(
                    fail, **resource_params
                )
                if not is_valid:
                    raise ValueError(f"Failed validation, reasons: {msg}")
                elif len(msg) > 0:
                    logger.warning(f"Failed validation, reasons: {msg}")

        for array in self.arrays.values():
            try:
                array.validate()
            except ValueError as e:
                if fail:
                    raise
                else:
                    logger.info(e)
        try:
            cluster_names = list(self._clusters.keys())
            if len(list(self._clusters.keys())) > 1:
                raise RuntimeError(
                    f"Workflow can only use one cluster. Found cluster_names={cluster_names}"
                )
            # check if workflow is valid
            self._dag.validate()
            self._matching_wf_args_diff_hash()
        except Exception as e:
            if fail:
                raise
            else:
                logger.info(e)

    def bind(self) -> None:
        """Get a workflow_id."""
        if self.is_bound:
            return

        self.validate(fail=False)
        for array in self.arrays.values():
            array.validate()
        self._dag.validate()
        self._matching_wf_args_diff_hash()

        # bind dag
        self._dag.bind(self._chunk_size)

        # bind workflow
        app_route = "/workflow"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "tool_version_id": self._tool_version.id,
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
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )

        self._workflow_id = response["workflow_id"]
        self._status = response["status"]
        self._newly_created = response["newly_created"]

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

    def get_cluster_by_name(self, cluster_name: str) -> Cluster:
        """Check if the cluster that the task specified is in the cache.

        If the cluster is not in the cache, create it and add to cache.
        """
        try:
            cluster = self._clusters[cluster_name]
        except KeyError:
            cluster = Cluster(cluster_name=cluster_name, requester=self.requester)
            cluster.bind()
            self._clusters[cluster_name] = cluster
        return cluster

    def _link_array_and_workflow(self, array: Array) -> None:
        template_name = array.task_template_version.task_template.template_name
        if template_name in self.arrays.keys():
            raise ValueError(
                f"An array for template_name={template_name} already exists on this workflow."
            )
        # add the references
        self.arrays[template_name] = array
        array.workflow = self

    def _create_workflow_run(
        self,
        resume: bool = False,
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
        client_wfr = ClientWorkflowRun(workflow=self, requester=self.requester)
        client_wfr.bind(reset_running_jobs, self._chunk_size)
        self._status = WorkflowStatus.QUEUED

        return client_wfr

    def _matching_wf_args_diff_hash(self) -> None:
        """Check that that an existing workflow does not contain different tasks.

        Check that an existing workflow with the same workflow_args does not have a
        different hash, this would indicate that thgat the workflow contains different tasks.
        """
        rc, response = self.requester.send_request(
            app_route=f"/workflow/{str(self.workflow_args_hash)}",
            message={},
            request_type="get",
        )
        bound_workflow_hashes = response["matching_workflows"]
        for task_hash, tool_version_id, dag_hash in bound_workflow_hashes:
            match = self._tool_version.id == tool_version_id and (
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
                app_route=app_route, message={}, request_type="get"
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

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha1()
        hash_value.update(str(hash(self._tool_version.id)).encode("utf-8"))
        hash_value.update(str(self.workflow_args_hash).encode("utf-8"))
        hash_value.update(str(self.task_hash).encode("utf-8"))
        hash_value.update(str(hash(self._dag)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    def __repr__(self) -> str:
        """A representation string for a Workflow instance."""
        repr_string = (
            f"Workflow(workflow_args={self.workflow_args}, " f"name={self.name}"
        )
        try:
            repr_string += f", workflow_id={self.workflow_id})"
        except AttributeError:
            # Workflow not yet bound so no ID to add to repr
            repr_string += ")"
        return repr_string
