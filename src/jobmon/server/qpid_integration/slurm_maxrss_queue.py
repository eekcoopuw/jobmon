"""Singleton for Max PSS Queue."""
import logging
import queue
from typing import Union


logger = logging.getLogger(__name__)


class MaxrssQ:
    """Singleton Queue for maxpss."""

    _q = None
    # Add an exit point
    keep_running = True

    def __init__(self, maxsize: int = 1000000) -> None:
        """Initialize queue."""
        if MaxrssQ._q is None:
            MaxrssQ._q = queue.Queue(maxsize=maxsize)

    @staticmethod
    def get() -> Union[tuple, None]:
        """Get an item from the queue."""
        try:
            return MaxrssQ._q.get_nowait()  # type: ignore
        except queue.Empty:
            logger.debug("Maxpss queue is empty")
            return None

    @staticmethod
    def put(execution_id: int, age: int = 0) -> None:
        """Put execution id in the queue."""
        try:
            MaxrssQ._q.put_nowait((execution_id, age))  # type: ignore
        except queue.Full:
            logger.warning("Queue is full")

    @staticmethod
    def get_size() -> int:
        """Get the size of the queue."""
        return MaxrssQ._q.qsize()  # type: ignore

    def empty_q(self) -> None:
        """This is for unit testing."""
        while self.get_size() > 0:
            self.get()
