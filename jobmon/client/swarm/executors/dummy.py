import random

from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.job_management.executor_job import ExecutorJob


class DummyExecutor(Executor):

    def execute(self, job: ExecutorJob, job_instance_id: int) -> int:
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        executor_id = random.randint(1, int(1e7))
        return executor_id
