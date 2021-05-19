from typing import Callable, Optional
from jobmon.cluster_type.base import ClusterQueue, ClusterResources


class SequentialQueue(ClusterQueue):

    def __init__(self, queue_id: int, queue_name: str, parameters: dict):
        # Get the limits from DB in client
        self.queue_id = queue_id
        self.name = queue_name
        self.parameters = parameters

    def validate(self, resource, value, fail=False):
        """Ensure cores requested isn't more than available on that
        node.
        """
        return "", value


class SequentialResources(ClusterResources):

    def memory(self):
        pass

    def runtime(self):
        pass

    def cores(self):
        pass

    def queue(self):
        pass

    def adjust(self, adjust_func: Optional[Callable], **kwargs):
        pass

    def scale_resource(self, resource: str):
        return resource
