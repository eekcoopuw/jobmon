import random

import structlog as logging
from typing import Tuple, Dict

from jobmon.client.execution.strategies.base import Executor, ExecutorParameters

logger = logging.getLogger(__name__)


class DummyExecutor(Executor):

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters, executor_ids) -> \
            Tuple[int, Dict[int, int]]:
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        logger.debug("This is the Dummy Executor")
        executor_id = random.randint(1, int(1e7))
        executor_ids[executor_id] = 0
        return executor_id, executor_ids
