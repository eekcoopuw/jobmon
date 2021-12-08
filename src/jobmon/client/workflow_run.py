"""The workflow run is an instance of a workflow."""
import getpass
import logging
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from jobmon import __version__
from jobmon.client.client_config import ClientConfig
from jobmon.client.task import Task
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import InvalidResponse, WorkflowNotResumable
from jobmon.requester import http_request_ok, Requester


if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

logger = logging.getLogger(__name__)


class WorkflowRun(object):
    """WorkflowRun enables tracking for multiple runs of a single Workflow.

    A Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(
        self,
        workflow: Workflow,
        requester: Optional[Requester] = None,
        workflow_run_heartbeat_interval: int = 30,
        heartbeat_report_by_buffer: float = 3.1,
    ) -> None:
        """Initialize client WorkflowRun."""
        # set attrs
        self._workflow = workflow
        self.user = getpass.getuser()

        if requester is None:
            requester = Requester(ClientConfig.from_defaults().url)
        self.requester = requester
        self.heartbeat_interval = workflow_run_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer

        # get an id for this workflow run
        self.workflow_run_id = self._register_workflow_run()

        # workflow was created successfully
        self.status = WorkflowRunStatus.REGISTERED

    @property
    def workflow_id(self):
        self._workflow.workflow_id

    def bind(
        self,
        tasks: Dict[int, Task],
        reset_if_running: bool = True,
        chunk_size: int = 500,
    ) -> Dict[int, Task]:
        """Link this workflow run with the workflow and add all tasks."""
        next_report_increment = (
            self.heartbeat_interval * self.heartbeat_report_by_buffer
        )
        current_wfr_id, current_wfr_status = self._link_to_workflow(
            next_report_increment
        )
        # we did not successfully link. returned workflow_run_id is not the same as this ID
        if self.workflow_run_id != current_wfr_id:

            raise WorkflowNotResumable(
                "There is another active workflow run already for workflow_id "
                f"({self.workflow_id}). Found previous workflow_run_id/status: "
                f"{current_wfr_id}/{current_wfr_status}"
            )
        self.status = WorkflowRunStatus.LINKING
        # last heartbeat
        self._last_heartbeat: float = time.time()

        try:
            tasks = self._bind_tasks(tasks, reset_if_running, chunk_size)
        except Exception:
            self._update_status(WorkflowRunStatus.ABORTED)
            raise
        else:
            self._update_status(WorkflowRunStatus.BOUND)
        return tasks

    def _update_status(self, status: str) -> None:
        """Update the status of the workflow_run with whatever status is passed."""
        app_route = f"/workflow_run/{self.workflow_run_id}/update_status"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"status": status},
            request_type="put",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self.status = status

    def _register_workflow_run(self) -> int:
        # bind to database
        app_route = "/workflow_run"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={
                "workflow_id": self.workflow_id,
                "user": self.user,
                "jobmon_version": __version__,
            },
            request_type="post",
            logger=logger,
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"Invalid Response to {app_route}: {rc}")
        return response["workflow_run_id"]

    def _link_to_workflow(self, next_report_increment: float) -> Tuple[int, int]:
        app_route = f"/workflow_run/{self.workflow_run_id}/link"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"next_report_increment": next_report_increment},
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST  request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        return response["current_wfr"]

    def _log_heartbeat(self, next_report_increment: float) -> None:
        app_route = f"/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "next_report_increment": next_report_increment,
                "status": WorkflowRunStatus.LINKING,
            },
            request_type="post",
            logger=logger,
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST  request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        self._last_heartbeat = time.time()

    def _bind_tasks(
        self,
        tasks: Dict[int, Task],
        reset_if_running: bool = True,
        chunk_size: int = 500,
    ) -> Dict[int, Task]:
        app_route = "/task/bind_tasks"
        remaining_task_hashes = list(tasks.keys())

        while remaining_task_hashes:

            if (time.time() - self._last_heartbeat) > self.heartbeat_interval:
                self._log_heartbeat(
                    self.heartbeat_interval * self.heartbeat_report_by_buffer
                )

            # split off first chunk elements from queue.
            task_hashes_chunk = remaining_task_hashes[:chunk_size]
            remaining_task_hashes = remaining_task_hashes[chunk_size:]

            # send to server in a format of:
            # {<hash>:[workflow_id(0), node_id(1), task_args_hash(2), array_id(3),
            # name(4), command(5), max_attempts(6)], reset_if_running(7), task_args(8),
            # task_attributes(9)}
            # flat the data structure so that the server won't depend on the client
            task_metadata: Dict[int, List] = {}
            for task_hash in task_hashes_chunk:
                # Bind task resources first
                task = tasks[task_hash]
                task_resources_id = None
                try:
                    task.task_resources.bind()
                    task_resources_id = task.task_resources.id
                except AttributeError as e:
                    # task resources should only raise this error if callable is not None
                    if task.compute_resources_callable is None:
                        raise e

                task_metadata[task_hash] = [
                    task.node.node_id,
                    task.task_args_hash,
                    task.array_id,
                    task_resources_id,
                    task.name,
                    task.command,
                    task.max_attempts,
                    reset_if_running,
                    task.task_args,
                    task.task_attributes,
                    task.resource_scales,
                    task.fallback_queues,
                ]
            parameters = {
                "workflow_id": self.workflow_id,
                "tasks": task_metadata,
            }
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message=parameters,
                request_type="put",
                logger=logger,
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f"Unexpected status code {return_code} from PUT "
                    f"request through route {app_route}. Expected code "
                    f"200. Response content: {response}"
                )

            # populate returned values onto task dict
            return_tasks = response["tasks"]
            for k in return_tasks.keys():
                task = tasks[int(k)]
                task.task_id = return_tasks[k][0]
                task.initial_status = return_tasks[k][1]

        return tasks

    def bind_arrays(self) -> None:
        """Add the arrays to the database.

        Done sequentially instead of in bulk, since scaling not assumed to be a problem
        with arrays.
        """
        for array in self._workflow.arrays.values():
            cluster = self._get_cluster_by_name(array.cluster_name)
            # Create a task resources object and bind to the array
            task_resources = cluster.create_valid_task_resources(
                resource_params=array.compute_resources,
                task_resources_type_id=TaskResourcesType.VALIDATED,
            )
            task_resources.bind(TaskResourcesType.VALIDATED)
            array.set_task_resources(task_resources)
            array.bind(workflow_id=self.workflow_id, cluster_id=cluster.id)
