"""Dummy Executor fakes execution for testing purposes."""
import random
from typing import Dict, Tuple

from jobmon.client.execution.strategies.base import Executor, ExecutorParameters

import structlog as logging

logger = logging.getLogger(__name__)


class DummyExecutor(Executor):
    """The Dummy Executor fakes the execution of a Task and acts as though it succeeded."""

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters, executor_ids) -> \
            Tuple[int, Dict[int, int]]:
        """Run a fake execution of the task."""
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        logger.debug("This is the Dummy Executor")
        executor_id = random.randint(1, int(1e7))
        executor_ids[executor_id] = 0
        return executor_id, executor_ids
