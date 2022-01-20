from __future__ import annotations

from typing import Callable, List, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance


class DistributorCommand:

    def __init__(
        self,
        func: Callable[..., Tuple[Set[DistributorTaskInstance], List[DistributorCommand]]],
        *args,
        **kwargs
    ):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self._func(*self.args, **self.kwargs)
