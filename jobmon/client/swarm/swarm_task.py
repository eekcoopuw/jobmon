from http import HTTPStatus as StatusCodes
from typing import Callable

from jobmon.client import shared_requester
from jobmon.exceptions import InvalidResponse
from jobmon.serializers import SerializeSwarmTask
from jobmon.models.task_status import TaskStatus


class SwarmTask(object):
    """The class that bridges the gap between a task and it's bound Task"""

    def __init__(self, task_id: int, status: str, task_args_hash: int,
                 executor_parameters: Callable = None, max_attempts: int = 3,
                 placeholder: bool = False, requester=shared_requester):
        """
        Link task and job

        Args
            task_id (int): id of task object from bound db object
            status (str): status of task object
            task_args_hash (int): hash of unique task arguments
            executor_parameters (Callable): callable to be executed when Task
                is ready to be run and resources can be assigned
            max_attempts (int): maximum number of task_instances before failure
            placeholder (bool): if this is a "shell" bound task from a
                previous run of the same workflow to hold the spot with the
                correct status or if it is really bound
            requester (Requester): requester to use to contact the flask
                services
        """
        self.task_id = task_id
        self.status = status

        self.upstream_bound_tasks = set()
        self.downstream_bound_tasks = set()

        self.executor_parameters = executor_parameters
        self.max_attempts = max_attempts
        self.task_args_hash = task_args_hash

        self.requester = requester
        self.placeholder = placeholder

        # once the callable is evaluated, the resources should be saved here
        self.bound_parameters: list = []

    @classmethod
    def from_wire(cls, wire_tuple: tuple):
        kwargs = SerializeSwarmTask.kwargs_from_wire(wire_tuple)
        return cls(task_id=kwargs["task_id"], status=kwargs["status"],
                   task_args_hash=kwargs["task_args_hash"])

    @property
    def is_bound(self):
        return not self.placeholder

    @property
    def all_upstreams_done(self):
        """Return a bool of if upstreams are done or not"""
        return all([u.is_done for u in self.upstream_tasks])

    @property
    def is_done(self):
        """Return a book of if this task is done or now"""
        return self.status == TaskStatus.DONE

    @property
    def downstream_tasks(self):
        """Return list of downstream tasks"""
        return list(self.downstream_bound_tasks)

    @property
    def upstream_tasks(self):
        """Return a list of upstream tasks"""
        return list(self.upstream_bound_tasks)

    def update_task(self, max_attempts: int):
        self.max_attempts = max_attempts

        msg = {'max_attempts': max_attempts}
        self.requester.send_request(
            app_route=f'/task/{self.task_id}/update_task',
            message=msg,
            request_type='post'
        )

    def queue_task(self):
        """Transition a task to the Queued for Instantiation status in the db
        """
        rc, _ = self.requester.send_request(
            app_route=f'/task/{self.task_id}/queue',
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(f"{rc}: Could not queue task")
        self.status = TaskStatus.QUEUED_FOR_INSTANTIATION
        return rc
