from jobmon.cluster_type.base import ClusterQueue
from typing import Any


class MultiprocessQueue(ClusterQueue):

    def __init__(self, queue_id: int, queue_name: str, parameters: dict):
        # Get the limits from DB in client
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resource(self, resource, value: Any, fail=False):
        """Ensure cores requested isn't more than available on that
        node.
        """
        return ""

    @property
    def queue_id(self):
        return self._queue_id

    @property
    def queue_name(self):
        return self._queue_name

    @property
    def parameters(self):
        return self._parameters

    @property
    def required_resources(self):
        return []
