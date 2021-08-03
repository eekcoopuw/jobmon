"""The client for the Multiprocess executor."""
from typing import Any, Dict, List, Tuple

from jobmon.cluster_type.base import ClusterQueue


class MultiprocessQueue(ClusterQueue):
    """Implementation of the multiprocess executor queue, derived from ClusterQueue."""

    def __init__(self, queue_id: int, queue_name: str, parameters: Dict) -> None:
        """Intialization of the multiprocess queue.

        Get the limits from the database in the client.
        """
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resources(self, **kwargs) -> Tuple[bool, str, Dict]:
        """Ensure cores requested isn't more than available on that node."""
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
        """No required resources specified for dummy executor, return empty list."""
        return []
