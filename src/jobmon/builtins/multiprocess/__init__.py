"""jobmon built-in."""
from typing import Type

from jobmon.cluster_type import ClusterDistributor, ClusterQueue, ClusterWorkerNode


def get_cluster_queue_class() -> Type[ClusterQueue]:
    """Return the queue class for the Multiprocess executor."""
    from jobmon.builtins.multiprocess.multiproc_queue import MultiprocessQueue

    return MultiprocessQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    """Return the cluster distributor for the Multiprocess executor."""
    from jobmon.builtins.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )

    return MultiprocessDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    """Return the cluster worker node class for the Multiprocess executor."""
    from jobmon.builtins.multiprocess.multiproc_distributor import (
        MultiprocessWorkerNode,
    )

    return MultiprocessWorkerNode
