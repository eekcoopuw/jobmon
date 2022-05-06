from __future__ import annotations

from typing import Any, Dict, List, Optional

from jobmon.cluster_type import ClusterQueue, ConcreteResource


class ConcreteMultiprocResource(ConcreteResource):
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
        """Return the concrete resources."""
        return self._resources

    @classmethod
    def adjust_and_create_concrete_resource(
        cls: Any,
        expected_queue: ClusterQueue,
        existing_resources: Dict,
        fallback_queues: Optional[List[ClusterQueue]],
        resource_scales: Optional[Dict[str, float]],
    ) -> ConcreteMultiprocResource:
        """No adjustment defined for multiprocess execution. Return original parameters."""
        return cls(queue=expected_queue, resources=existing_resources)
