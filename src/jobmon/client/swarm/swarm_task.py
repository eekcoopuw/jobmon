from __future__ import annotations

from http import HTTPStatus as StatusCodes
from typing import Callable, Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.constants import ExecutorParameterSetType, TaskStatus
from jobmon.exceptions import CallableReturnedInvalidObject, InvalidResponse
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeSwarmTask

import structlog as logging


logger = logging.getLogger(__name__)


class SwarmTask(object):

    def __init__(self, task_id: int, status: str, task_args_hash: int,
                 executor_parameters: Optional[Callable] = None,
                 max_attempts: int = 3, requester: Optional[Requester] = None) -> None:
        """
        Implementing swarm behavior of tasks

        Args
            task_id: id of task object from bound db object
            status: status of task object
            task_args_hash: hash of unique task arguments
            executor_parameters: callable to be executed when Task is ready to be run and
            resources can be assigned
            max_attempts: maximum number of task_instances before failure
            requester_url (str): url to communicate with the flask services.
        """
        self.task_id = task_id
        self.status = status

        self.upstream_swarm_tasks: Set[SwarmTask] = set()
        self.downstream_swarm_tasks: Set[SwarmTask] = set()

        self.executor_parameters_callable = executor_parameters
        self.max_attempts = max_attempts
        self.task_args_hash = task_args_hash

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        # once the callable is evaluated, the resources should be saved here
        self.bound_parameters: list = []

        self.num_upstreams_done: int = 0

    @staticmethod
    def from_wire(wire_tuple: tuple, swarm_tasks_dict: Dict[int, SwarmTask]) -> SwarmTask:
        kwargs = SerializeSwarmTask.kwargs_from_wire(wire_tuple)
        swarm_tasks_dict[kwargs["task_id"]].status = kwargs["status"]
        return swarm_tasks_dict[kwargs["task_id"]]

    def to_wire(self) -> tuple:
        return SerializeSwarmTask.to_wire(self.task_id, self.status)

    @property
    def all_upstreams_done(self) -> bool:
        """Return a bool of if upstreams are done or not"""
        if (self.num_upstreams_done >= len(self.upstream_tasks)):
            logger.debug(f"task id: {self.task_id} is checking all upstream tasks")
            return all([u.is_done for u in self.upstream_tasks])
        else:
            return False

    @property
    def is_done(self) -> bool:
        """Return a book of if this task is done or now"""
        return self.status == TaskStatus.DONE

    @property
    def downstream_tasks(self) -> List[SwarmTask]:
        """Return list of downstream tasks"""
        return list(self.downstream_swarm_tasks)

    @property
    def upstream_tasks(self) -> List[SwarmTask]:
        """Return a list of upstream tasks"""
        return list(self.upstream_swarm_tasks)

    def get_executor_parameters(self):
        """return an instance of executor parameters"""
        return self.executor_parameters_callable(self)

    def queue_task(self) -> int:
        """Transition a task to the Queued for Instantiation status in the db
        """
        rc, _ = self.requester.send_request(
            app_route=f'/swarm/task/{self.task_id}/queue',
            message={},
            request_type='post',
            logger=logger
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(f"{rc}: Could not queue task")
        self.status = TaskStatus.QUEUED_FOR_INSTANTIATION
        return rc

    @staticmethod
    def adjust_resources(self) -> ExecutorParameters:
        """Function from Job Instance Factory that adjusts resources and then
           queues them, this should also incorporate resource binding if they
           have not yet been bound"""
        logger.debug("Job in A state, adjusting resources before queueing")

        # get the most recent parameter set
        exec_param_set = self.bound_parameters[-1]
        only_scale = list(exec_param_set.resource_scales.keys())

        app_route = f'/worker/task/{self.task_id}/most_recent_ti_error'
        return_code, response = self.requester.send_request(
            app_route=f'/worker/task/{self.task_id}/most_recent_ti_error',
            message={},
            request_type='get',
            logger=logger
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        # check if we are only scaling runtime.
        # TODO: this logic should be in ExecutorParameters.adjust since it is
        # SGE specific
        if ('max_runtime' in response and 'max_runtime_seconds' in only_scale):
            only_scale = ['max_runtime_seconds']
        logger.debug(
            f"Only going to scale the following resources: {only_scale}")
        resources_adjusted = {'only_scale': only_scale}
        exec_param_set.adjust(**resources_adjusted)
        return exec_param_set

    def bind_executor_parameters(self, executor_parameter_set_type: str) -> None:
        """bind executor parameters to db"""

        # evaluate callable and validate it is the right type of object
        executor_parameters = self.get_executor_parameters()
        if not isinstance(executor_parameters, ExecutorParameters):
            raise CallableReturnedInvalidObject(
                "The function called to return executor_parameters did not "
                "return the expected ExecutorParameters object, it is of type"
                f"{type(executor_parameters)}")
        self.bound_parameters.append(executor_parameters)

        # validate the values
        if executor_parameter_set_type == ExecutorParameterSetType.VALIDATED:
            executor_parameters.validate()

        # bind to db
        app_route = f'/swarm/task/{self.task_id}/update_resources'
        msg = {'parameter_set_type': executor_parameter_set_type}
        msg.update(executor_parameters.to_wire())
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=msg,
            request_type='post',
            logger=logger
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')
