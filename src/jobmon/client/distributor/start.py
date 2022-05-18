"""Start up distributing process."""
from typing import Optional

from jobmon.client.distributor.distributor_config import DistributorConfig
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.cluster import Cluster
from jobmon.requester import Requester


def get_distributor_service(
    cluster_name: str,
    distributor_config: Optional[DistributorConfig] = None,
) -> DistributorService:
    """Set up and return distributor object."""
    if distributor_config is None:
        distributor_config = DistributorConfig.from_defaults()
    requester = Requester(distributor_config.url)

    # get the cluster_distributor API
    cluster = Cluster.get_cluster(cluster_name, requester)
    cluster_interface = cluster.get_distributor()

    distributor_service = DistributorService(
        cluster_interface=cluster_interface,
        requester=requester,
        task_instance_heartbeat_interval=distributor_config.task_instance_heartbeat_interval,
        heartbeat_report_by_buffer=distributor_config.heartbeat_report_by_buffer,
        distributor_poll_interval=distributor_config.distributor_poll_interval,
        worker_node_entry_point=distributor_config.worker_node_entry_point,
    )
    return distributor_service
