"""jobmon_uge."""
from typing import Type
from jobmon.cluster_type import (
    ClusterQueue,
    ClusterDistributor,
    ClusterWorkerNode,
)


def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.builtins.sequential.seq_queue import SequentialQueue

    return SequentialQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    from jobmon.builtins.sequential.seq_distributor import SequentialDistributor

    return SequentialDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    from jobmon.builtins.sequential.seq_distributor import SequentialWorkerNode

    return SequentialWorkerNode
