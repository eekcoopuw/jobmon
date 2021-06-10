from typing import Any

from jobmon.cluster_type.base import ClusterQueue


class SequentialQueue(ClusterQueue):

    def __init__(self, queue_id: int, queue_name: str, required_resources: dict):
        # Get the limits from DB in client
        self.queue_id = queue_id
        self.queue_name = queue_name
        self.required_resources = required_resources

    def validate_resource(self, resource, value: Any, fail=False):
        """Ensure cores requested isn't more than available on that
        node.
        """
        return ""

    def queue_id(self):
        return self.queue_id

    def queue_name(self):
        return self.queue_name

    def required_resources(self):
        return self.required_resources
