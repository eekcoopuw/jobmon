from typing import Optional, List

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import ExecutorParameters
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.job_status import JobStatus
from jobmon.serializers import SerializeExecutorJob
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class ExecutorJob:

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
        """
        This is a Job object used on the RESTful API client side
        when constructing job instances.

        Args:
            dag_id: dag_id associated with this job
            job_id: job_id associated with this job
            name: name associated with this job
            job_hash: hash of command for this job
            command: what command to run when executing
            status: job status  associated with this job
            executor_parameters: Executor parameters class associated with the
                current executor for this job
            last_nodename: where this job last executed
            last_process_group_id: what was the linux process group id of the
                last instance of this job
            requester: requester for communicating with central services
        """

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
        logger.info("Instantiate job jid {}".format(kwargs["job_id"]))
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
                context_args=kwargs["context_args"],
                resource_scales=kwargs["resource_scales"],
                hard_limits=kwargs["hard_limits"]),
            requester=requester)
        return executor_job

    def queue_job(self) -> None:
        """Transition a job to the Queued for Instantiation status"""
        app_route = f"/job/{self.job_id}/queue"
        rc, _ = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='post')
        self.status == JobStatus.QUEUED_FOR_INSTANTIATION
