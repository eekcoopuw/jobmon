"""Start up distributing process."""
from typing import Optional

from jobmon.client.distributor.distributor_config import DistributorConfig
from jobmon.client.distributor.task_instance_distributor import TaskInstanceDistributor

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import Requester


def get_task_instance_distributor(workflow_id: int, workflow_run_id: int,
                    distributor: ClusterDistributor,
                    distributor_config: Optional[DistributorConfig] = None) -> TaskInstanceDistributor:
    """Set up and return distributor object."""
    if distributor_config is None:
        distributor_config = DistributorConfig.from_defaults()

    requester = Requester(distributor_config.url)
    task_instance_distributor = TaskInstanceDistributor(
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        distributor=distributor,
        requester=requester,
        workflow_run_heartbeat_interval=distributor_config.workflow_run_heartbeat_interval,
        task_heartbeat_interval=distributor_config.task_heartbeat_interval,
        heartbeat_report_by_buffer=distributor_config.heartbeat_report_by_buffer,
        n_queued=distributor_config.n_queued,
        distributor_poll_interval=distributor_config.distributor_poll_interval,
        jobmon_command=distributor_config.jobmon_command
    )
    return task_instance_distributor
