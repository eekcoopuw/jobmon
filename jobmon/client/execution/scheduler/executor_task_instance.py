from typing import Optional

from jobmon.client.swarm import shared_requester
from jobmon.client.requests.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.serializers import SerializeExecutorTaskInstance
from jobmon.client.swarm import SwarmLogging as logging


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

    def __init__(self,
                 task_instance_id: int,
                 executor: Executor,
                 requester: Requester,
                 executor_id: Optional[int] = None):

        self.task_instance_id = task_instance_id
        self.executor_id = executor_id

        # interfaces to the executor and server
        self.executor = executor
        self.requester = requester

    @classmethod
    def from_wire(cls,
                  wire_tuple: tuple,
                  executor: Executor,
                  requester: Requester = shared_requester
                  ) -> "ExecutorTaskInstance":
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
                   executor=executor,
                   executor_id=kwargs["executor_id"],
                   requester=requester)

    @classmethod
    def register_task_instance(cls,
                               task_id: int,
                               executor: Executor,
                               requester: Requester
                               ) -> "ExecutorTaskInstance":
        """register a new task instance for an existing task_id

        Args:
            task_id (int): the task_id to register this instance with
            executor (Executor): which executor to schedule this task on
            requester: requester for communicating with central services
        """

        rc, response = requester.send_request(
            app_route='/task_instance',
            message={'task_id': task_id,
                     'executor_type': executor.__class__.__name__},
            request_type='post')
        return cls.from_wire(response['task_instance'], executor=executor)

    def register_no_exec_id(self, executor_id: int) -> None:
        """register that submission failed with the central service

        Args:
            executor_id: placeholder executor id. generall -9999
        """
        self.executor_id = executor_id
        self.requester.send_request(
            app_route=f'/task_instance/{self.task_instance_id}/log_no_exec_id',
            message={'executor_id': executor_id},
            request_type='post')

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
        self.requester.send_request(
            app_route=app_route,
            message={'executor_id': str(executor_id),
                     'next_report_increment': next_report_increment},
            request_type='post')

    def log_error(self) -> None:
        """Log an error from the executor loops"""
        if self.executor_id is None:
            raise ValueError("executor_id cannot be None during log_error")
        executor_id: int = self.executor_id
        logger.info("log_error for executor_id {}".format(executor_id))
        try:
            error_state, msg = self.executor.get_remote_exit_info(executor_id)
        except RemoteExitInfoNotAvailable:
            msg = ("Unknown error caused task to be lost")
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
        self.requester.send_request(
            app_route=(
                f"/task_instance/{self.task_instance_id}/"
                "log_error_reconciler"),
            message=message,
            request_type='post')
