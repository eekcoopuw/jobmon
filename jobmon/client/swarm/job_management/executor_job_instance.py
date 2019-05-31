from typing import Optional

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.job_management.executor_job import ExecutorJob
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.job_instance_status import JobInstanceStatus
from jombon.serializers import SerializeExecutorJobInstance


class ExecutorJobInstance:
    """Object used for communicating with JSM from the executor node

    Args:
        job_instance_id (int): a job_instance_id
        executor (Executor): an instance of an Executor or a subclass
        executor_id (int, optional): the executor_id associated with this
            job_instance
        job (ExecutorJob, optional): the executor_job stub associated with this
            job_instance
        requester (Requester, optional): a requester to communicate with
            the JSM. default is shared requester
    """

    def __init__(self,
                 job_instance_id: int,
                 executor: Executor,
                 executor_id: Optional[int] = None,
                 job: Optional[ExecutorJob] = None,
                 requester: Requester = shared_requester):

        self.job_instance_id = job_instance_id
        self._executor_id = executor_id
        self.job = job

        # interfaces to the executor and server
        self.executor = executor
        self.requester = shared_requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, executor: Executor):
        """create an instance from json that the JQS returns"""
        return cls(executor=executor,
                   **SerializeExecutorJobInstance.kwargs_from_wire(wire_tuple))

    @classmethod
    def register_job_instance(cls, job: ExecutorJob, executor: Executor):
        rc, response = shared_requester.send_request(
            app_route='/job_instance',
            message={'job_id': str(job.job_id),
                     'executor_type': executor.__class__.__name__},
            request_type='post')
        job_instance_id = response['job_instance_id']
        return cls(job_instance_id=job_instance_id, executor=executor,
                   job=job)

    def register_no_exec_id(self, executor_id: int):
        self._executor_id = executor_id
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_no_exec_id',
            message={'executor_id': executor_id},
            request_type='post')

    def register_submission_to_batch_executor(self, executor_id: int,
                                              next_report_increment: float):
        self._executor_id = executor_id
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_executor_id',
            message={'executor_id': str(executor_id),
                     'next_report_increment': next_report_increment},
            request_type='post')

    def log_error(self) -> None:
        """Log an error from the swarm node"""
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
