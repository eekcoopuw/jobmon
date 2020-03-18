import random

from jobmon.client.execution.strategies.base import (Executor,
                                                     ExecutorParameters)
from jobmon.client import ClientLogging as logging

logger = logging.getLogger(__name__)


class DummyExecutor(Executor):

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        logger.warning("This is the Dummy Executor")
        executor_id = random.randint(1, int(1e7))
        return executor_id
