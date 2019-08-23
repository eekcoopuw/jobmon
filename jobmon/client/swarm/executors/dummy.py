import random

from jobmon.client.swarm.executors import Executor, ExecutorParameters


class DummyExecutor(Executor):

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        executor_id = random.randint(1, int(1e7))
        return executor_id
