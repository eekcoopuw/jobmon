from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.serializers import SerializableExecutorJob


class ExecutorJob:
    """
    This is a Job object used on the RESTful API client side
    when constructing job instances.
    """

    def __init__(self, dag_id: int, job_id: int, name: str, job_hash: int,
                 command: str, status: str, max_runtime_seconds: int,
                 context_args: str, queue: str, num_cores: int,
                 m_mem_free: str, j_resource: str, last_nodename: str,
                 last_process_group_id: int,
                 requester: Requester = shared_requester):
        self.dag_id = dag_id
        self.job_id = job_id
        self.name = name
        self.job_hash = job_hash
        self.command = command
        self.status = status
        self.max_runtime_seconds = max_runtime_seconds
        self.context_args = context_args
        self.queue = queue
        self.num_cores = num_cores
        self.m_mem_free = m_mem_free
        self.j_resource = j_resource
        self.last_nodename = last_nodename
        self.last_process_group_id = last_process_group_id

        self.requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple,
                  requester: Requester = shared_requester):
        return cls(requester=requester,
                   **SerializableExecutorJob.kwargs_from_wire(wire_tuple))

    def update_executor_parameter_set(self, parameter_set_type: str) -> None:
        self.requester.send_request(
            app_route=f'/job/{self.job_id}/change_resources',
            message={'parameter_set_type': parameter_set_type,
                     'max_runtime_seconds': self.max_runtime_seconds,
                     'context_args': self.context_args,
                     'queue': self.queue,
                     'num_cores': self.num_cores,
                     'm_mem_free': self.m_mem_free,
                     'j_resource': self.j_resource},
            request_type='post')
