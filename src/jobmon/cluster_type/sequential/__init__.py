"""jobmon_uge."""
from typing import Type
from jobmon.cluster_type.base import (
    ClusterQueue,
    ClusterDistributor,
    ClusterWorkerNode,
    ConcreteResource,
)


def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.cluster_type.sequential.seq_queue import SequentialQueue

    return SequentialQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor

    return SequentialDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    from jobmon.cluster_type.sequential.seq_distributor import SequentialWorkerNode

    return SequentialWorkerNode


def get_concrete_resource_class() -> Type[ConcreteResource]:
    from jobmon.cluster_type.multiprocess.multiproc_concrete_resource import (
        ConcreteMultiprocResource,
    )

    return ConcreteMultiprocResource
