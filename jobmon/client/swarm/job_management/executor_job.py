
class ExecutorJob:
    """
    This is a Job object used on the RESTful API client side
    when constructing job instances.
    """

    def __init__(self, dag_id: int, job_id: int, name: str, job_hash: int,
                 command: str, status: str, max_runtime_seconds: int,
                 context_args: str, queue: str, num_cores: int,
                 m_mem_free: str, j_resource: str, last_nodename: str,
                 last_process_group_id: int):
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

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'],
                   job_id=dct['job_id'],
                   name=dct['name'],
                   job_hash=int(dct['job_hash']),
                   command=dct['command'],
                   status=dct['status'],

                   max_runtime_seconds=dct['max_runtime_seconds'],
                   context_args=dct['context_args'],
                   queue=dct['queue'],
                   num_cores=dct['num_cores'],
                   m_mem_free=dct['m_mem_free'],
                   j_resource=dct['j_resource'],

                   last_nodename=dct['last_nodename'],
                   last_process_group_id=dct['last_process_group_id'])
