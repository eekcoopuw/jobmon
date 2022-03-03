from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from jobmon.cluster_type.base import ClusterQueue, ConcreteResource


class ConcreteSequentialResource(ConcreteResource):
    """A version of a private constructor in Python."""

    def __init__(self, queue: ClusterQueue, resources: Dict) -> None:
        """Always assumed to be valid.

        Don't call init directly, this object should be created by validate or adjust.
        """
        self._queue = queue
        self._resources = resources

    @property
    def queue(self) -> ClusterQueue:
        """Return the cluster queue."""
        return self._queue

    @property
    def resources(self) -> Dict[str, Any]:
        """Return the resources."""
        return self._resources

    @classmethod
    def validate_and_create_concrete_resource(
        cls: Any, queue: ClusterQueue, requested_resources: Dict
    ) -> Tuple[bool, str, ConcreteSequentialResource]:
        """Validate resources against the specified queue.

        Args:
            queue: ClusterQueue to validate against.
            requested_resources: the compute resources the user requested.
        """
        is_valid, msg, valid_resources = queue.validate_resources(**requested_resources)
        return is_valid, msg, cls(queue=queue, resources=valid_resources)

    @classmethod
    def adjust_and_create_concrete_resource(
        cls: Any,
        expected_queue: ClusterQueue,
        existing_resources: Dict,
        fallback_queues: Optional[List[ClusterQueue]],
        resource_scales: Optional[Dict[str, float]],
    ) -> ConcreteSequentialResource:
        """No adjustment defined for sequential execution. Return original parameters."""
        return cls(queue=expected_queue, resources=existing_resources)
