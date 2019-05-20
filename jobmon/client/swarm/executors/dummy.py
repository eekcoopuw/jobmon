import random

from jobmon.client.swarm.executors import Executor


class DummyExecutor(Executor):

    def execute(self, job_instance):
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        executor_id = random.randint(1, 1e7)
        return executor_id
