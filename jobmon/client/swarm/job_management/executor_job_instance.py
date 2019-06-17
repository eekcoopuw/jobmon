from typing import Optional

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.serializers import SerializeExecutorJobInstance


class ExecutorJobInstance:
    """Object used for communicating with JSM from the executor node

    Args:
        job_instance_id (int): a job_instance_id
        executor (Executor): an instance of an Executor or a subclass
        executor_id (int, optional): the executor_id associated with this
            job_instance
        requester (Requester, optional): a requester to communicate with
            the JSM. default is shared requester
    """

    def __init__(self,
                 job_instance_id: int,
                 executor: Executor,
                 executor_id: Optional[int] = None,
                 requester: Requester = shared_requester):

        self.job_instance_id = job_instance_id
        self.executor_id = executor_id

        # interfaces to the executor and server
        self.executor = executor
        self.requester = shared_requester

    @classmethod
    def from_wire(cls,
                  wire_tuple: tuple,
                  executor: Executor,
                  requester: Requester = shared_requester
                  ) -> "ExecutorJobInstance":
        """create an instance from json that the JQS returns

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                job. format = serializers.SerializeExecutorJob.to_wire()
            executor (Executor): which executor this job instance is
                being run on
            requester (Requester, shared_requester): requester for
                communicating with central services

        Returns:
            ExecutorJobInstance
        """
        kwargs = SerializeExecutorJobInstance.kwargs_from_wire(wire_tuple)
        return cls(job_instance_id=kwargs["job_instance_id"],
                   executor=executor,
                   executor_id=kwargs["executor_id"],
                   requester=requester)

    @classmethod
    def register_job_instance(cls,
                              job_id: int,
                              executor: Executor
                              ) -> "ExecutorJobInstance":
        """register a new job instance for an existing job_id

        Args:
            job_id (int): the job_id to register this instance with
            executor (Executor): which executor to schedule this job on

        Returns:
            ExecutorJobInstance
        """

        rc, response = shared_requester.send_request(
            app_route='/job_instance',
            message={'job_id': job_id,
                     'executor_type': executor.__class__.__name__},
            request_type='post')
        return cls.from_wire(response['job_instance'], executor=executor)

    def register_no_exec_id(self, executor_id: int) -> None:
        """register that submission failed with the central service

        Args:
            executor_id (int): placeholder executor id. generall -9999
        """
        self._executor_id = executor_id
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_no_exec_id',
            message={'executor_id': executor_id},
            request_type='post')

    def register_submission_to_batch_executor(self, executor_id: int,
                                              next_report_increment: float
                                              ) -> None:
        """register the submission of a new job instance to batch execution

        Args:
            executor (int): executor id created by executor for this job
                instance
            next_report_increment (float): how many seconds to wait for
                report or status update before considering the job lost
        """
        self._executor_id = executor_id
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_executor_id',
            message={'executor_id': str(executor_id),
                     'next_report_increment': next_report_increment},
            request_type='post')

    def log_error(self) -> None:
        """Log an error from the executor loops"""
        if self._executor_id is None:
            raise ValueError("executor_id cannot be None during log_error")
        executor_id: int = self._executor_id
        try:
            error_state, msg = self.executor.get_remote_exit_info(executor_id)
        except RemoteExitInfoNotAvailable:
            msg = ("Unknow error caused job to be lost")
            error_state = JobInstanceStatus.UNKNOWN_ERROR

        # this is the 'happy' path. The executor gives us a concrete error for
        # the lost job
        if error_state == JobInstanceStatus.RESOURCE_ERROR:
            message = {
                "error_message": msg,
                "error_state": error_state,
                "executor_id": executor_id
            }
        # this is the 'unhappy' path. We are giving up discovering the exit
        # state and moving the job into unknown error state
        else:
            message = {
                "error_message": msg,
                "error_state": error_state
            }
        self.requester.send_request(
            app_route=(
                f'/job_instance/{self.job_instance_id}/log_error_reconciler'),
            message=message,
            request_type='post')
