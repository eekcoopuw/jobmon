"""Singleton for Max PSS Queue."""
import heapq
import logging
from typing import List, Union

from jobmon.server.usage_integration.usage_utils import QueuedTI

logger = logging.getLogger(__name__)


class UsageQ:
    """Singleton Queue for maxpss."""

    _q: List[QueuedTI] = []
    _maxsize: int = 1000000000
    keep_running: bool = True

    @staticmethod
    def get() -> Union[tuple, None]:
        """Get an item from the queue."""
        if len(UsageQ._q) == 0:
            logger.debug("Maxpss queue is empty")
            return None
        item = heapq.heappop(UsageQ._q)
        return item, item.age

    @staticmethod
    def put(queued_ti: QueuedTI, age: int = 0) -> None:
        """Put execution id in the queue."""
        if len(UsageQ._q) < UsageQ._maxsize:
            queued_ti.age = age
            UsageQ._q.append(queued_ti)  # type: ignore
        else:
            logger.warning("Queue is full")

    @staticmethod
    def get_size() -> int:
        """Get the size of the queue."""
        return len(UsageQ._q)  # type: ignore

    @staticmethod
    def empty_q() -> None:
        """This is for unit testing."""
        UsageQ._q = []
