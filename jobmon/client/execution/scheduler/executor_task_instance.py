from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Optional

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.requests.requester import Requester
from jobmon.client.execution.strategies.base import Executor
from jobmon.exceptions import RemoteExitInfoNotAvailable, InvalidResponse
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.serializers import SerializeExecutorTaskInstance


logger = logging.getLogger(__name__)


class ExecutorTaskInstance:
    """Object used for communicating with JSM from the executor node

    Args:
        task_instance_id (int): a task_instance_id
        executor (Executor): an instance of an Executor or a subclass
        executor_id (int, optional): the executor_id associated with this
            task_instance
        requester (Requester, optional): a requester to communicate with
            the JSM. default is shared requester
    """

    def __init__(self, task_instance_id: int, workflow_run_id: int,
                 executor: Executor, requester: Requester,
                 executor_id: Optional[int] = None):

        self.task_instance_id = task_instance_id
        self.workflow_run_id = workflow_run_id
        self.executor_id = executor_id

        # interfaces to the executor and server
        self.executor = executor
        self.requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, executor: Executor,
                  requester: Requester = shared_requester
                  ) -> ExecutorTaskInstance:
        """create an instance from json that the JQS returns

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                task. format = serializers.SerializeExecutorTask.to_wire()
            executor (Executor): which executor this task instance is
                being run on
            requester (Requester, shared_requester): requester for
                communicating with central services
                  requester: Requester

        Returns:
            ExecutorTaskInstance
        """
        kwargs = SerializeExecutorTaskInstance.kwargs_from_wire(wire_tuple)
        return cls(task_instance_id=kwargs["task_instance_id"],
                   workflow_run_id=kwargs["workflow_run_id"],
                   executor=executor,
                   executor_id=kwargs["executor_id"],
                   requester=requester)

    @classmethod
    def register_task_instance(cls, task_id: int, workflow_run_id: int,
                               executor: Executor, requester: Requester
                               ) -> ExecutorTaskInstance:
        """register a new task instance for an existing task_id

        Args:
            task_id (int): the task_id to register this instance with
            executor (Executor): which executor to schedule this task on
            requester: requester for communicating with central services
        """

        app_route = '/task_instance'
        return_code, response = requester.send_request(
            app_route=app_route,
            message={'task_id': task_id,
                     'workflow_run_id': workflow_run_id,
                     'executor_type': executor.__class__.__name__},
            request_type='post')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        return cls.from_wire(response['task_instance'], executor=executor)

    def register_no_executor_id(self, executor_id: int) -> None:
        """register that submission failed with the central service

        Args:
            executor_id: placeholder executor id. generall -9999
        """
        self.executor_id = executor_id

        app_route = (
            f'/task_instance/{self.task_instance_id}/log_no_executor_id')
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'executor_id': executor_id},
            request_type='post')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def register_submission_to_batch_executor(self, executor_id: int,
                                              next_report_increment: float
                                              ) -> None:
        """register the submission of a new task instance to batch execution

        Args:
            executor_id (int): executor id created by executor for this task
                instance
            next_report_increment: how many seconds to wait for
                report or status update before considering the task lost
        """
        self.executor_id = executor_id

        app_route = f'/task_instance/{self.task_instance_id}/log_executor_id'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'executor_id': str(executor_id),
                     'next_report_increment': next_report_increment},
            request_type='post')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def log_error(self) -> None:
        """Log an error from the executor loops"""
        if self.executor_id is None:
            raise ValueError("executor_id cannot be None during log_error")
        executor_id: int = self.executor_id
        logger.info(f"log_error for executor_id {executor_id}")
        try:
            error_state, msg = self.executor.get_remote_exit_info(executor_id)
        except RemoteExitInfoNotAvailable:
            msg = ("Unknown error caused task instance to be lost")
            logger.warning(msg)
            error_state = TaskInstanceStatus.UNKNOWN_ERROR

        # this is the 'happy' path. The executor gives us a concrete error for
        # the lost task
        if error_state == TaskInstanceStatus.RESOURCE_ERROR:
            logger.info(
                f"log_error resource error for executor_id {executor_id}")
            message = {
                "error_message": msg,
                "error_state": error_state,
                "executor_id": executor_id
            }
        # this is the 'unhappy' path. We are giving up discovering the exit
        # state and moving the task into unknown error state
        else:
            logger.info(f"Giving up discovering the exit state for "
                        f"executor_id {executor_id} with error_state "
                        f"{error_state}")
            message = {
                "error_message": msg,
                "error_state": error_state
            }

        app_route = (
                f"/task_instance/{self.task_instance_id}/"
                "log_error_reconciler")
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type='post')
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def dummy_executor_task_instance_run_and_done(self) -> None:
        """For all instances other than the DummyExecutor, the worker node task instance should
        be the one logging the done status. Since the DummyExecutor doesn't actually run
        anything, the task_instance_scheduler needs to mark it done so that execution can
        proceed through the DAG"""
        if self.executor.__class__.__name__ != "DummyExecutor":
            logger.error("Cannot directly log a task instance done unless using the Dummy "
                         "Executor")
        logger.info("Moving the job to running, then done so that dependencies can proceed to "
                    "mock a successful dag traversal process")
        run_app_route = f'/task_instance/{self.task_instance_id}/log_running'
        run_message = {'process_group_id': '0', 'next_report_increment': 60}
        return_code, response = self.requester.send_request(
            app_route=run_app_route,
            message=run_message,
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {run_app_route}. Expected '
                                  f'code 200. Response content: {response}')
        done_app_route = f"/task_instance/{self.task_instance_id}/log_done"
        done_message = {'nodename': 'DummyNode', 'executor_id': self.executor_id}
        return_code, response = self.requester.send_request(
            app_route=done_app_route,
            message=done_message,
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {done_app_route}. Expected '
                                  f'code 200. Response content: {response}')

