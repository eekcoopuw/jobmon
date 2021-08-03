"""The client for the Sequential Executor."""
from typing import Dict, List, Tuple

from jobmon.cluster_type.base import ClusterQueue


class SequentialQueue(ClusterQueue):
    """Implementation of the sequential executor queue, derived from ClusterQueue."""

    def __init__(self, queue_id: int, queue_name: str, parameters: dict) -> None:
        """Intialization of SequentialQueue.

        Get the limits from database in client.
        """
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resources(self, **kwargs) -> Tuple[bool, str, Dict]:
        """No resources defined for sequential execution. All resources valid"""
        return True, "", kwargs

    @property
    def queue_id(self) -> int:
        """Return the ID of the queue."""
        return self._queue_id

    @property
    def queue_name(self) -> str:
        """Return the name of the queue."""
        return self._queue_name

    @property
    def parameters(self) -> Dict:
        """Return the dictionary of parameters."""
        return self._parameters

    @property
    def required_resources(self) -> List:
        """No specified required resources for sequential, return empty list."""
        return []
