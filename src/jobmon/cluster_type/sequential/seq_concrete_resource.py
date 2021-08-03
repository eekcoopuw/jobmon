from __future__ import annotations
from typing import Dict, Tuple

from jobmon.cluster_type.base import ConcreteResource, ClusterQueue


class ConcreteSequentialResource(ConcreteResource):
    # A version of a private constructor in Python
    # Init shouldn't be called directly, otherwise we don't know resources are valid
    # Note that this means the return type of validate and adjust are actually
    # ConcreteUGEResource.__Implementation. Unsure if that's bad practice

    def __init__(self, queue: ClusterQueue, valid_resources: Dict):
        """
        Always assumed to be valid.
        Don't call init directly, this object should be created by validate or adjust.
        """
        self.queue = queue
        self.resources = valid_resources

    @classmethod
    def validate_and_create_concrete_resource(
            cls, queue: ClusterQueue, requested_resources: Dict
    ) -> Tuple[bool, str, ConcreteSequentialResource]:
        is_valid, msg, valid_resources = queue.validate_resources(**requested_resources)
        return is_valid, msg, cls(queue=queue, valid_resources=valid_resources)

    @classmethod
    def adjust_and_create_concrete_resource(
            cls, expected_queue: ClusterQueue, existing_resources: Dict, **kwargs
    ) -> ConcreteSequentialResource:
        """No adjustment defined for sequential execution. Return original parameters"""
        return cls(queue=expected_queue, valid_resources=existing_resources)
