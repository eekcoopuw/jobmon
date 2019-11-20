from http import HTTPStatus as StatusCodes

from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.exceptions import InvalidResponse
from jobmon.models.job_status import JobStatus
from jobmon.models.attributes.constants import job_attribute


class BoundTask(object):
    """The class that bridges the gap between a task and it's bound Job"""

    def __init__(self, client_task, bound_task: SwarmJob, requester):
        """
        Link task and job

        Args
            client_task (obj): obj of a class inherited from ExecutableTask
            bound_task (obj): obj of type SwarmJob after binding to the db

        """
        self.job_id = bound_task.job_id
        self.status = bound_task.status
        self._client_task = client_task

        self.upstream_bound_tasks = set()
        self.downstream_bound_tasks = set()

        self.requester = requester

        if client_task:
            self.executor_parameters = client_task.executor_parameters
            self.max_attempts = client_task.max_attempts
            self.hash = client_task.hash
        else:
            self.executor_parameters = None
            self.max_attempts = 3
            self.hash = None

        # once the callable is evaluated, the resources should be saved here
        self.bound_parameters: list = []

    @property
    def is_bound(self):
        return (self._client_task is not None) and (self.hash is not None)

    @property
    def all_upstreams_done(self):
        """Return a bool of if upstreams are done or not"""
        return all([u.is_done for u in self.upstream_tasks])

    @property
    def is_done(self):
        """Return a book of if this job is done or now"""
        return self.status == JobStatus.DONE

    @property
    def downstream_tasks(self):
        """Return list of downstream tasks"""
        # return [self._jlm.bound_task_from_task(task)
        #         for task in self._task.downstream_tasks]
        return list(self.downstream_bound_tasks)

    @property
    def upstream_tasks(self):
        """Return a list of upstream tasks"""
        return list(self.upstream_bound_tasks)

    def update_task(self, max_attempts: int):
        self.max_attempts = max_attempts

        msg = {'max_attempts': max_attempts}
        self.requester.send_request(
            app_route=f'/job/{self.job_id}/update_job',
            message=msg,
            request_type='post'
        )

    def add_job_attribute(self, attribute_type, value):
        """
        Create a job attribute entry in the database.

        Args:
            attribute_type (int): attribute_type id from
                                  job_attribute_type table
            value (int): value associated with attribute

        Raises:
            ValueError: If the args are not valid or if the
                        attribute is used for usage data and
                        cannot be configured on the user side.
                        attribute_type should be int and
                        value should be convertible to int
                        or be string for TAG attribute
        """
        user_cant_config = [job_attribute.WALLCLOCK, job_attribute.CPU,
                            job_attribute.IO, job_attribute.MAXRSS]
        if attribute_type in user_cant_config:
            raise ValueError(f"Invalid attribute configuration for "
                             f"{attribute_type} with "
                             f"name: {type(attribute_type).__name__}, user "
                             f"input not used to configure attribute value")
        elif not isinstance(attribute_type, int):
            raise ValueError(f"Invalid attribute type: {attribute_type}, "
                             f"{type(attribute_type).__name__}")
        elif (not attribute_type == job_attribute.TAG and not int(value))\
                or (attribute_type == job_attribute.TAG and
                    not isinstance(value, str)):
            raise ValueError(f"Invalid value type: {value}, "
                             f"{type(value).__name__}")
        else:
            rc, job_attribute_id = self.requester.send_request(
                app_route='/job_attribute',
                message={'job_id': str(self.job_id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return job_attribute_id

    def queue_job(self):
        """Transition a job to the Queued for Instantiation status in the db"""
        rc, _ = self.requester.send_request(
            app_route=f'/job/{self.job_id}/queue',
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(f"{rc}: Could not queue job")
        self.status = JobStatus.QUEUED_FOR_INSTANTIATION
        return rc
