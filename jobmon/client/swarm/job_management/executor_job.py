import logging
from typing import Optional

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import ExecutorParameters
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.job_status import JobStatus
from jobmon.serializers import SerializeExecutorJob


logger = logging.getLogger(__name__)


class ExecutorJob:
    """
    This is a Job object used on the RESTful API client side
    when constructing job instances.

    Args:
        dag_id (int): dag_id associated with this job
        job_id (int): job_id associated with this job
        name (str): name associated with this job
        job_hash (int): hash of command for this job
        command (str): what command to run when executing
        status (str): job status  associated with this job
        executor_parameters (ExecutorParameters): Executor parameters class
            associated with the current executor for this job
        last_nodename (str, Optional[str]): where this job last executed
        last_process_group_id (int, Optional[int]) what was the linux process
            group id of the last instance of this job
        requester (Requester, shared_requester): requester for communicating
            with central services
    """

    # this API should always match what's returned by
    # serializers.SerializeExecutorJob
    def __init__(self,
                 dag_id: int,
                 job_id: int,
                 name: str,
                 job_hash: int,
                 command: str,
                 status: str,
                 executor_parameters: ExecutorParameters,
                 last_nodename: Optional[str] = None,
                 last_process_group_id: Optional[int] = None,
                 requester: Requester = shared_requester):

        self.dag_id = dag_id
        self.job_id = job_id
        self.name = name
        self.job_hash = job_hash
        self.command = command
        self.status = status
        self.last_nodename = last_nodename
        self.last_process_group_id = last_process_group_id

        self.executor_parameters = executor_parameters

        self.requester = requester

    @classmethod
    def from_wire(cls,
                  wire_tuple: tuple,
                  executor_class: str,
                  requester: Requester = shared_requester
                  ) -> 'ExecutorJob':
        """construct instance from wire format the JQS gives

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                job. format = serializers.SerializeExecutorJob.to_wire()
            executor_class (str): which executor class this job instance is
                being run on
            requester (Requester, shared_requester): requester for
                communicating with central services
        """

        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeExecutorJob.kwargs_from_wire(wire_tuple)

        # instantiate job
        executor_job = cls(
            dag_id=kwargs["dag_id"],
            job_id=kwargs["job_id"],
            name=kwargs["name"],
            job_hash=kwargs["job_hash"],
            command=kwargs["command"],
            status=kwargs["status"],
            last_nodename=kwargs["last_nodename"],
            last_process_group_id=kwargs["last_process_group_id"],
            executor_parameters=ExecutorParameters(
                executor_class=executor_class,
                num_cores=kwargs["num_cores"],
                queue=kwargs["queue"],
                max_runtime_seconds=kwargs["max_runtime_seconds"],
                j_resource=kwargs["j_resource"],
                m_mem_free=kwargs["m_mem_free"],
                context_args=kwargs["context_args"]),
            requester=requester)
        return executor_job

    def update_executor_parameter_set(self, parameter_set_type: str =
                                      ExecutorParameterSetType.ADJUSTED
                                      ) -> None:
        """update the resources for a given job in the db

        Args:
            parameter_set_type (str): models.executor_parameter_set_type value
        """

        # TODO: refactor for common API between executor parameter types.
        # somehow infer what paremeters need scaled based executor

        # adjust parameters
        adjustment_factor = 0.5
        param_adjustment = {'num_cores': adjustment_factor,
                            'm_mem_free': adjustment_factor,
                            'max_runtime_seconds': adjustment_factor}
        self.executor_parameters.adjust(**param_adjustment)

        msg = {'parameter_set_type': parameter_set_type}
        msg.update(self.executor_parameters.to_wire())
        self.requester.send_request(
            app_route=f'/job/{self.job_id}/update_resources',
            message=msg,
            request_type='post')

    def queue_job(self) -> None:
        """Transition a job to the Queued for Instantiation status"""
        app_route = f"/job/{self.job_id}/queue"
        rc, _ = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='post')
        self.status == JobStatus.QUEUED_FOR_INSTANTIATION
