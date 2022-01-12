from typing import Callable, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_service import DistributorService


class ProcessorResult:

    def __init__(
        self,
        processed_task_instances: Set[DistributorTaskInstance],

    ):
        self.processed_task_instances = processed_task_instances
        self.new_processor_
