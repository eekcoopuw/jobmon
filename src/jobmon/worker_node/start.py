"""Start up distributing process."""
from typing import Optional

from jobmon.cluster import Cluster
from jobmon.requester import Requester
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


def get_worker_node_task_instance(
    cluster_name: str,
    task_instance_id: Optional[int] = None,
    array_id: Optional[int] = None,
    batch_number: Optional[int] = None,
    worker_node_config: Optional[WorkerNodeConfig] = None,
) -> WorkerNodeTaskInstance:
    """Set up and return WorkerNodeTaskInstance object."""
    if worker_node_config is None:
        worker_node_config = WorkerNodeConfig.from_defaults()

    requester = Requester(worker_node_config.url)

    cluster = Cluster.get_cluster(cluster_name)
    worker_node_interface = cluster.get_worker_node()

    worker_node_task_instance = WorkerNodeTaskInstance(
        cluster_interface=worker_node_interface,
        task_instance_id=task_instance_id,
        array_id=array_id,
        batch_number=batch_number,
        heartbeat_interval=worker_node_config.task_instance_heartbeat_interval,
        report_by_buffer=worker_node_config.heartbeat_report_by_buffer,
        requester=requester,
    )
    return worker_node_task_instance
