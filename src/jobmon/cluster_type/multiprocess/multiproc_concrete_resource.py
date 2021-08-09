from __future__ import annotations
from typing import Any, Dict, Tuple

from jobmon.cluster_type.base import ConcreteResource, ClusterQueue


class ConcreteMultiprocResource(ConcreteResource):
    # A version of a private constructor in Python
    # Init shouldn't be called directly, otherwise we don't know resources are valid
    # Note that this means the return type of validate and adjust are actually
    # ConcreteUGEResource.__Implementation. Unsure if that's bad practice

    def __init__(self, queue: ClusterQueue, valid_resources: Dict):
        """
        Always assumed to be valid.
        Don't call init directly, this object should be created by validate or adjust.
        """
        self._queue = queue
        self._resources = valid_resources

    @property
    def queue(self) -> ClusterQueue:
        return self._queue

    @property
    def resources(self) -> Dict[str, Any]:
        return self._resources

    @classmethod
    def validate_and_create_concrete_resource(
            cls, queue: ClusterQueue, requested_resources: Dict
    ) -> Tuple[bool, str, ConcreteResource]:
        is_valid, msg, valid_resources = queue.validate_resources(**requested_resources)
        return is_valid, msg, cls(queue=queue, valid_resources=valid_resources)

    @classmethod
    def adjust_and_create_concrete_resource(
            cls, expected_queue: ClusterQueue, existing_resources: Dict, **kwargs
    ) -> ConcreteMultiprocResource:
        """No adjustment defined for multiprocess execution. Return original parameters"""
        return cls(queue=expected_queue, valid_resources=existing_resources)
