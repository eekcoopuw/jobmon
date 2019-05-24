from datetime import datetime
from typing import Optional

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.job_instance_status import JobInstanceStatus


class SwarmJobInstance:
    """Object used for communicating with JSM from the swarm node

    Args:
        job_instance_id (int): a job_instance_id
        executor_id (int): an executor_id associated with this job_instance
        executor (Executor): an instance of an Executor or a subclass
        workflow_run_id (int, optional): a workflow_run_id associated with
            this job instance. default is None
        nodename (str, optional): the nodename that this job instance is
            running on. default is None
        process_group_id (int, optional): the process group id that this
            job instance was running under on the remote host. default is
            None
        job_id (int, optional): the job_id associated with this job
            instance. default is None
        dag_id (int, optional): the dag_id associated with this job
            instance. default is None
        status (int, optional): the status of this job. default is None
        status_date (int, optional): the timestamp when the status was last
            updated. default is None
        requester (Requester, optional): a requester to communicate with
            the JSM. default is shared requester
    """

    def __init__(self,
                 job_instance_id: int,
                 executor_id: int,
                 executor: Executor,
                 workflow_run_id: Optional[int]=None,
                 nodename: Optional[str]=None,
                 process_group_id: Optional[int]=None,
                 job_id: Optional[int]=None,
                 dag_id: Optional[int]=None,
                 status: Optional[int]=None,
                 status_date: Optional[datetime]=None,
                 requester: Requester=shared_requester):

        self.job_instance_id = job_instance_id
        self.executor_id = executor_id

        # interfaces to the executor and server
        self.executor = executor
        self.requester = shared_requester

        # these attributes do not affect any current functionality but are
        # returned from the job_query_service
        self.workflow_run_id = workflow_run_id
        self.nodename = nodename
        self.process_group_id = process_group_id
        self.job_id = job_id
        self.dag_id = dag_id
        self.status = status
        self.status_date = status_date

    @classmethod
    def from_wire(cls, dct, executor):
        """create an instance from json that the JQS returns"""
        return cls(job_instance_id=dct['job_instance_id'],
                   executor=executor,
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   nodename=dct['nodename'],
                   process_group_id=dct['process_group_id'],
                   job_id=dct['job_id'],
                   dag_id=dct['dag_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def log_error(self) -> None:
        """Log an error from the swarm node"""
        try:
            error_state, msg = self.executor.get_remote_exit_info(
                self.executor_id)
        except RemoteExitInfoNotAvailable:
            msg = ("Unknow error caused job to be lost")
            error_state = JobInstanceStatus.UNKNOWN_ERROR

        # this is the 'happy' path. The executor gives us a concrete error for
        # the lost job
        if error_state == JobInstanceStatus.RESOURCE_ERROR:
            message = {
                "error_message": msg,
                "error_state": error_state,
                "executor_id": self.executor_id
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
