from typing import Optional, List, Set, TYPE_CHECKING


from jobmon.client.distributor.distributor_workflow_run2 import DistributorWorkflowRun
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance


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

        self.distributors = distributor
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

    def instantiate_task_instances(self, workflow_run: DistributorWorkflowRun):
        """given a workflow_run, instantiate all queued task instances."""
        # TODO: rename _n_queued
        processed_task_instances: Set[DistributorTaskInstance] = set()
        new_task_instances = workflow_run.instantiate_queued_task_instances(self._n_queued)
        processed_task_instances.update(new_task_instances)

        while len(new_task_instances) == self._n_queued:
            new_task_instances = workflow_run.instantiate_queued_task_instances(self._n_queued)
            processed_task_instances.update(new_task_instances)

        workflow_run.update_state_map(processed_task_instances)

    def launch_task_instances(self, workflow_run: DistributorWorkflowRun):
        processed_task_instances: Set[DistributorTaskInstance] = set()
        task_instance_batches = workflow_run.get_task_instance_batches_for_launch()

        while task_instance_batches:
            task_instance_batch = task_instance_batches.pop(0)
            # get an element of the batch
            task_instance = next(iter(task_instance_batch))
            cluster = task_instance.cluster_id
            # TODO: how do we translate task_resource_id into requested resources??? We can
            # pass the payload from the server when we get the task instance but that is
            # inefficient for arrays. Maybe just lookup in the submit method and keep a
            # registry on the workflow run?
            # task_resources = task_instance.task_resources_id
            if len(task_instance_batches) > 1:
                try:
                    task_instances = workflow_run.launch_array_instance(task_instance_batch,
                                                                        cluster)
                    processed_task_instances.update(task_instances)
                except NotImplementedError:
                    # unpack set into single element tuples if not implemented by cluster
                    task_instance_batches.extend(list(zip(task_instance_batches)))
            else:
                # unpack single element
                task_instance = workflow_run.launch_task_instance(task_instance, cluster)
                processed_task_instances.append(task_instance)

        workflow_run.update_state_map(processed_task_instances)

    def monitor_launched_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: purge queueing errors. check distributor for liveliness?
        pass

    def monitor_running_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: Can EQW happen at this stage. check distributor for liveliness?
        pass

    def process_errored_task_instances(self, workflow_run: DistributorWorkflowRun):
        # TODO: EQW, unknown error, other?
        pass

    def _heartbeat_interrupt(self) -> bool:
        return False

    def iterate_distributor(self):
        self.instantiate_task_instances(self.workflow_run)  # INSTANTIATED
        self.launch_task_instance(self.workflow_run)
        self.monitor_launched_task_instances(self.workflow_run)
        self.monitor_running_task_instances(self.workflow_run)
        self.process_errored_task_instances(self.workflow_run)

        # TODO: When to syncronize status with db. Perhaps once for each queue/method
        # TODO: Should the monitoring of launched and running be treated different? Perhaps a
        # higher priority.
