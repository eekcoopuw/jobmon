from abc import abstractproperty
from typing import Callable, Dict, List, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_service import DistributorService


class TaskInstanceProcessor:

    def __init__(self, func: Callable[DistributorService, Set[DistributorTaskInstance]]):
        self._func = func

    def __call__(self, distributor_service):
        processed_task_instances = self._func(distributor_service)

    @abstractproperty
    def processed_task_instances(self) -> Set[DistributorTaskInstance]:
        pass

    @abstractproperty
    def new_work(self) -> Dict[str, List[Callable[DistributorService, None]]]:
        pass
