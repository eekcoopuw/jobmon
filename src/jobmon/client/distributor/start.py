"""Start up distributing process."""
from typing import Optional

from jobmon.client.distributor.distributor_config import DistributorConfig
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import Requester


def get_distributor_service(workflow_id: int, workflow_run_id: int,
                            distributor: ClusterDistributor,
                            distributor_config: Optional[DistributorConfig] = None) \
        -> DistributorService:
    """Set up and return distributor object."""
    if distributor_config is None:
        distributor_config = DistributorConfig.from_defaults()

    requester = Requester(distributor_config.url)
    distributor_service = DistributorService(
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        distributor=distributor,
        requester=requester,
        workflow_run_heartbeat_interval=distributor_config.workflow_run_heartbeat_interval,
        task_instance_heartbeat_interval=distributor_config.task_instance_heartbeat_interval,
        heartbeat_report_by_buffer=distributor_config.heartbeat_report_by_buffer,
        n_queued=distributor_config.n_queued,
        distributor_poll_interval=distributor_config.distributor_poll_interval,
        worker_node_entry_point=distributor_config.worker_node_entry_point
    )
    return distributor_service
