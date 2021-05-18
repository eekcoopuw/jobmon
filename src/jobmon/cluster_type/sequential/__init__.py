"""jobmon_uge."""
from typing import Type
from jobmon.cluster_type.base import (ClusterResources, ClusterQueue, ClusterDistributor,
                                      ClusterWorkerNode)


def get_cluster_resources_class() -> Type[ClusterResources]:
    from jobmon.cluster_type.sequential.seq_client import SequentialResources
    return SequentialResources


def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.cluster_type.sequential.seq_client import SequentialQueue
    return SequentialQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    return SequentialDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    from jobmon.cluster_type.sequential.seq_distributor import SequentialWorkerNode
    return SequentialWorkerNode
