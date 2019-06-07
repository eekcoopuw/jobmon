import inspect
import logging
from typing import Type, Optional, Dict, Tuple

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import ExecutorParameters
from jobmon.models.job_status import JobStatus
from jobmon.serializers import SerializeExecutorJob


logger = logging.getLogger(__name__)


class ExecutorJob:
    """
    This is a Job object used on the RESTful API client side
    when constructing job instances.
    """

    def __init__(self, dag_id: int, job_id: int, name: str, job_hash: int,
                 command: str, status: str, last_nodename: Optional[str],
                 last_process_group_id: Optional[int],
                 executor_parameters: ExecutorParameters,
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
    def parse_constructor_kwargs(cls, kwarg_dict: Dict) -> Tuple[Dict, Dict]:
        argspec = inspect.getfullargspec(cls.__init__)
        constructor_kwargs = {}
        for arg in argspec.args:
            if arg in kwarg_dict:
                constructor_kwargs[arg] = kwarg_dict.pop(arg)
        return kwarg_dict, constructor_kwargs

    @classmethod
    def from_wire(
            cls, wire_tuple: tuple, executor_class: str,
            requester: Requester = shared_requester) -> "ExecutorJob":
        """construct instance from wire format"""
        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeExecutorJob.kwargs_from_wire(wire_tuple)

        # separates the kwargs we recieved from the wire used for constructing
        # ExecutorJob
        kwargs, executor_job_kwargs = cls.parse_constructor_kwargs(kwargs)

        executor_job = cls(
            requester=requester,
            executor_parameters=ExecutorParameters(
                executor_class=executor_class, from_original=False, **kwargs),
            **executor_job_kwargs)
        return executor_job

    def update_executor_parameter_set(self, parameter_set_type: str) -> None:
        # TODO: refactor for common API between executor parameter types

        # adjust parameters
        adjustment_factor = 0.5
        param_adjustment = {'num_cores': adjustment_factor,
                            'm_mem_free': adjustment_factor,
                            'max_runtime_seconds': adjustment_factor}
        adjusted_params = self.executor_parameters.get_adjusted(param_adjustment)
        self.executor_parameters = adjusted_params

        msg = {'parameter_set_type': parameter_set_type}
        msg.update(self.executor_parameters.to_wire())
        self.requester.send_request(
            app_route=f'/job/{self.job_id}/update_resources',
            message=msg,
            request_type='post')

    def queue_job(self):
        """Transition a job to the Queued for Instantiation status in the db

        Args:
            job (ExecutorJob): the id of the job to be queued
        """
        app_route = f"/job/{self.job_id}/queue"
        rc, _ = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='post')
        self.status == JobStatus.QUEUED_FOR_INSTANTIATION
