from typing import Optional


from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import Requester


class DistributorService:

    def __init__(
        self,
        distributor: ClusterDistributor,
        requester: Requester,
        workflow_run_heartbeat_interval: int = 30,
        task_instance_heartbeat_interval: int = 90,
        heartbeat_report_by_buffer: float = 3.1,
        n_queued: int = 100,
        distributor_poll_interval: int = 10,
        worker_node_entry_point: Optional[str] = None
    ) -> None:

        # operational args
        self._worker_node_entry_point = worker_node_entry_point
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self._n_queued = n_queued
        self._distributor_poll_interval = distributor_poll_interval

        self.requester = requester

    def set_workflow_run(self, workflow_id: int, workflow_run_id: int):
        self.workflow_run = DistributorWorkflowRun(
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            workflow_run_heartbeat_interval=self._workflow_run_heartbeat_interval,
            task_instance_heartbeat_interval=self._task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=self._heartbeat_report_by_buffer,
            requester=self.requester
        )

    def instantiate_queued_tasks(self):
        self.workflow_run.instantiate_queued_tasks(self.n_queued)

    def launched_task_instances(self):
        self.workflow_run

