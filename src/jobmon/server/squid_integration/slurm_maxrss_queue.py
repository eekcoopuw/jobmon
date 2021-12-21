"""Singleton for Max PSS Queue."""
import logging
import queue
from typing import Union

from jobmon.server.squid_integration.squid_utils import QueuedTI


logger = logging.getLogger(__name__)


class MaxrssQ:
    """Singleton Queue for maxpss."""

    _q = None
    _maxsize = 1000000

    @staticmethod
    def get() -> Union[QueuedTI, None]:
        """Get an item from the queue."""
        if MaxrssQ._q is None:
            MaxrssQ._q = queue.Queue(maxsize=MaxrssQ._maxsize)
        try:
            return MaxrssQ._q.get_nowait()  # type: ignore
        except queue.Empty:
            logger.debug("Maxpss queue is empty")
            return None

    @staticmethod
    def put(queued_ti: QueuedTI, age: int = 0) -> None:
        """Put execution id in the queue."""
        try:
            if MaxrssQ._q is None:
                MaxrssQ._q = queue.Queue(maxsize=MaxrssQ._maxsize)
            MaxrssQ._q.put_nowait((queued_ti, age))  # type: ignore
        except queue.Full:
            logger.warning("Queue is full")

    @staticmethod
    def get_size() -> int:
        """Get the size of the queue."""
        if MaxrssQ._q is None:
            MaxrssQ._q = queue.Queue(maxsize=MaxrssQ._maxsize)
        return MaxrssQ._q.qsize()  # type: ignore

    def empty_q(self) -> None:
        """This is for unit testing."""
        while self.get_size() > 0:
            self.get()
