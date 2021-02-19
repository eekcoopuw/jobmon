"""Singleton for Max PSS Queue."""
import logging
import queue


logger = logging.getLogger(__name__)


class MaxpssQ:
    """Singleton Queue for maxpss."""

    _q = None
    # Add an exit point
    keep_running = True

    def __init__(self, maxsize: int = 1000000):
        """Initialize queue."""
        if MaxpssQ._q is None:
            MaxpssQ._q = queue.Queue(maxsize=maxsize)

    def get(self):
        """Get an item from the queue."""
        try:
            return MaxpssQ._q.get_nowait()
        except queue.Empty:
            logger.debug("Maxpss queue is empty")
            return None

    def put(self, execution_id, age=0):
        """Put execution id in the queue."""
        try:
            MaxpssQ._q.put_nowait((execution_id, age))
        except queue.Full:
            logger.warning("Queue is full")

    def get_size(self):
        """Get the size of the queue."""
        return MaxpssQ._q.qsize()

    def empty_q(self):
        """This is for unit testing."""
        while self.get_size() > 0:
            self.get()
