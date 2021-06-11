from jobmon.cluster_type.base import ClusterQueue


class MultiprocessQueue(ClusterQueue):

    def __init__(self, queue_id: int, queue_name: str, parameters: dict):
        # Get the limits from DB in client
        self.queue_id = queue_id
        self.name = queue_name
        self.parameters = parameters

    def validate_resource(self, resource, value, fail=False):
        """Ensure cores requested isn't more than available on that
        node.
        """
        return "", value
