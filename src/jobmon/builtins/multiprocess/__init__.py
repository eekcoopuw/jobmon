"""jobmon built-in."""
from typing import Type
from jobmon.cluster_type import (
    ClusterQueue,
    ClusterDistributor,
    ClusterWorkerNode,
)


def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.builtins.multiprocess.multiproc_queue import MultiprocessQueue

    return MultiprocessQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    from jobmon.builtins.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )

    return MultiprocessDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    from jobmon.builtins.multiprocess.multiproc_distributor import (
        MultiprocessWorkerNode,
    )

    return MultiprocessWorkerNode
