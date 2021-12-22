"""Singleton for Max PSS Queue."""
import logging
import heapq
from typing import Union

from jobmon.server.squid_integration.squid_utils import QueuedTI

logger = logging.getLogger(__name__)


class MaxrssQ:
    """Singleton Queue for maxpss."""

    _q = []
    _maxsize = 1000000000
    keep_running = True

    @staticmethod
    def get() -> Union[tuple, None]:
        """Get an item from the queue."""
        if len(MaxrssQ._q) == 0:
            logger.debug("Maxpss queue is empty")
            return None
        item = heapq.heappop(MaxrssQ._q)
        return item, item.age


    @staticmethod
    def put(queued_ti: QueuedTI, age: int = 0) -> None:
        """Put execution id in the queue."""
        if len(MaxrssQ._q) < MaxrssQ._maxsize:
            queued_ti.age = age
            MaxrssQ._q.append(queued_ti)  # type: ignore
        else:
            logger.warning("Queue is full")

    @staticmethod
    def get_size() -> int:
        """Get the size of the queue."""
        return len(MaxrssQ._q)  # type: ignore

    @staticmethod
    def empty_q() -> None:
        """This is for unit testing."""
        MaxrssQ._q = []
