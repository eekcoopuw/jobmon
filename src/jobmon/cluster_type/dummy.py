"""Dummy Executor fakes execution for testing purposes."""
import random

#from jobmon.client.execution.strategies.base import Executor, ExecutorParameters
from typing import Dict, Any

import structlog as logging

from jobmon.cluster_type.base import ClusterDistributor

logger = logging.getLogger(__name__)


class DummyDistributor(ClusterDistributor):
    """The Dummy Executor fakes the execution of a Task and acts as though it succeeded."""

    def execute(self, command: str, name: str, requested_resources: Dict[str, Any]) -> int:
        """Run a fake execution of the task."""
        # in a real executor, this is where qsub would happen.
        # here, since it's a dummy executor, we just get a random num
        logger.debug("This is the Dummy Distributor")
        executor_id = random.randint(1, int(1e7))
        return executor_id
