from typing import Dict, List

from jobmon.cluster_type.base import ConcreteResource
from jobmon.cluster_type.multiprocess.multiproc_client import MultiprocessQueue

class ConcreteMultiprocResource(ConcreteResource):
    # A version of a private constructor in Python
    # Init shouldn't be called directly, otherwise we don't know resources are valid
    # Note that this means the return type of validate and adjust are actually
    # ConcreteUGEResource.__Implementation. Unsure if that's bad practice
    class __Implementation:

        def __init__(self, queue: MultiprocessQueue, valid_resources: Dict):
            """
            Always assumed to be valid.
            Don't call init directly, this object should be created by validate or adjust.
            """
            self.queue = queue
            self.resources = valid_resources

    def __new__(cls, *args, **kwargs):
        raise Exception("ConcreteMultiprocResource should not be initialized without validation, use the ",
                        "validate_and_create_concrete_resource or adjust methods to initialize")

    @classmethod
    def validate_and_create_concrete_resource(cls, queue: MultiprocessQueue, requested_resources: Dict):
        is_valid, msg, valid_resources = queue.validate_resource(**requested_resources)
        return is_valid, msg, cls.__Implementation(queue=queue, valid_resources=valid_resources)

    @classmethod
    def adjust(cls, expected_queue: MultiprocessQueue,
               existing_resources: Dict, *args, **kwargs):
        """No adjustment defined for multiprocess execution. Return original parameters"""
        return cls.__Implementation(queue=expected_queue, valid_resources=existing_resources)