"""jobmon built-in."""
from typing import Type
from jobmon.cluster_type.base import (
    ClusterQueue,
    ClusterDistributor,
    ClusterWorkerNode,
    ConcreteResource,
)


def get_cluster_queue_class() -> Type[ClusterQueue]:
    from jobmon.cluster_type.multiprocess.multiproc_queue import MultiprocessQueue

    return MultiprocessQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )

    return MultiprocessDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessWorkerNode,
    )

    return MultiprocessWorkerNode


def get_concrete_resource_class() -> Type[ConcreteResource]:
    from jobmon.cluster_type.multiprocess.multiproc_concrete_resource import (
        ConcreteMultiprocResource,
    )

    return ConcreteMultiprocResource
