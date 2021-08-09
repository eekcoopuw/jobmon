"""The workflow run is an instance of a workflow."""
import getpass
import logging
import time
from typing import Dict, List, Optional, Tuple

from jobmon import __version__
from jobmon.client.client_config import ClientConfig
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import InvalidResponse, WorkflowNotResumable
from jobmon.requester import http_request_ok, Requester


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

    def __init__(self, workflow_id: int, requester: Optional[Requester] = None,
                 workflow_run_heartbeat_interval: Optional[int] = None,
                 heartbeat_report_by_buffer: Optional[float] = None) -> None:
        """Initialize client WorkflowRun."""
        # set attrs
        self.workflow_id = workflow_id
        self.user = getpass.getuser()

        # set attrs from config
        need_client_config = (workflow_run_heartbeat_interval
                              is None or heartbeat_report_by_buffer is None
                              or requester is None)
        if need_client_config:
            client_config = ClientConfig.from_defaults()
        if requester is None:
            requester = Requester(client_config.url)
        self.requester = requester
        if workflow_run_heartbeat_interval is None:
            workflow_run_heartbeat_interval = client_config.workflow_run_heartbeat_interval
        self.heartbeat_interval = workflow_run_heartbeat_interval
        if heartbeat_report_by_buffer is None:
            heartbeat_report_by_buffer = client_config.heartbeat_report_by_buffer
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer

        # get an id for this workflow run
        self.workflow_run_id = self._register_workflow_run()

        # workflow was created successfully
        self.status = WorkflowRunStatus.REGISTERED

    def bind(self, tasks: Dict[int, Task], reset_if_running: bool = True,
             chunk_size: int = 500) -> Dict[int, Task]:
        """Link this workflow run with the workflow and add all tasks."""
        next_report_increment = self.heartbeat_interval * self.heartbeat_report_by_buffer
        current_wfr_id, current_wfr_status = self._link_to_workflow(next_report_increment)
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
        app_route = f'/swarm/workflow_run/{self.workflow_run_id}/update_status'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'status': status},
            request_type='put',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
        self.status = status

    def _register_workflow_run(self) -> int:
        # bind to database
        app_route = "/client/workflow_run"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={'workflow_id': self.workflow_id,
                     'user': self.user,
                     'jobmon_version': __version__,
                     },
            request_type='post',
            logger=logger
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"Invalid Response to {app_route}: {rc}")
        return response['workflow_run_id']

    def _link_to_workflow(self, next_report_increment: float) -> Tuple[int, int]:
        app_route = f"/client/workflow_run/{self.workflow_run_id}/link"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"next_report_increment": next_report_increment},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST  request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )
        return response['current_wfr']

    def _log_heartbeat(self, next_report_increment: float) -> None:
        app_route = f"/client/workflow_run/{self.workflow_run_id}/log_heartbeat"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"next_report_increment": next_report_increment},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST  request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )
        self._last_heartbeat = time.time()

    def _bind_tasks(self, tasks: Dict[int, Task], reset_if_running: bool = True,
                    chunk_size: int = 500) -> Dict[int, Task]:
        app_route = '/client/task/bind_tasks'
        parameters = {}
        remaining_task_hashes = list(tasks.keys())

        while remaining_task_hashes:

            if (time.time() - self._last_heartbeat) > self.heartbeat_interval:
                self._log_heartbeat(self.heartbeat_interval * self.heartbeat_report_by_buffer)

            # split off first chunk elements from queue.
            task_hashes_chunk = remaining_task_hashes[:chunk_size]
            remaining_task_hashes = remaining_task_hashes[chunk_size:]

            # send to server in a format of:
            # {<hash>:[workflow_id(0), node_id(1), task_args_hash(2), name(3),
            # command(4), max_attempts(5)], reset_if_running(6), task_args(7),
            # task_attributes(8)}
            # flat the data structure so that the server won't depend on the client
            task_metadata: Dict[int, List] = {}
            for task_hash in task_hashes_chunk:
                task_metadata[task_hash] = [
                    tasks[task_hash].node.node_id, str(tasks[task_hash].task_args_hash),
                    tasks[task_hash].name, tasks[task_hash].command,
                    tasks[task_hash].max_attempts, reset_if_running,
                    tasks[task_hash].task_args, tasks[task_hash].task_attributes,
                    tasks[task_hash].resource_scales,
                    tasks[task_hash].fallback_queues
                ]
            parameters = {
                "workflow_id": self.workflow_id,
                "tasks": task_metadata,
            }
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message=parameters,
                request_type='put',
                logger=logger,
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f'Unexpected status code {return_code} from PUT '
                    f'request through route {app_route}. Expected code '
                    f'200. Response content: {response}')

            # populate returned values onto task dict
            return_tasks = response["tasks"]
            for k in return_tasks.keys():
                task = tasks[int(k)]
                task.task_id = return_tasks[k][0]
                task.initial_status = return_tasks[k][1]

                # Bind the task resources
                try:
                    task.task_resources.bind(task.task_id)
                except AttributeError as e:
                    # task resources should only raise this error if callable is not None
                    if task.compute_resources_callable is None:
                        raise e

        return tasks
