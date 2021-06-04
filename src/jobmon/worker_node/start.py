"""Start up scheduling process."""
from typing import Optional

from jobmon.requester import Requester
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


def get_worker_node_task_instance(task_instance_id: int,
                 cluster_type_name: str,
                 worker_node_config: Optional[WorkerNodeConfig] = None) -> WorkerNodeTaskInstance:
    """Set up and return WorkerNodeTaskInstance object."""
    if worker_node_config is None:
        worker_node_config = WorkerNodeConfig.from_defaults()

    worker_node_task_instance = WorkerNodeTaskInstance(
        task_instance_id=task_instance_id,
        cluster_type_name=cluster_type_name,
        requester_url=worker_node_config.url
    )
    return worker_node_task_instance
