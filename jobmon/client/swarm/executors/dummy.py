import random

from jobmon.client.swarm.executors import Executor
from jobmon.models.job_instance import JobInstance


class DummyExecutor(Executor):

    def execute(self, job_instance: JobInstance) -> int:
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        executor_id = random.randint(1, int(1e7))
        return executor_id
