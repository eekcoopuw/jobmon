from __future__ import annotations

from typing import Callable, List, Optional

# from jobmon.cluster_type.base import ClusterDistributor


class DistributorCommand:
    def __init__(
        self, func: Callable[..., Optional[List[DistributorCommand]]], *args, **kwargs
    ):
        """A command to be run by the distributor service.

        Args:
            func: a callable which does work and optionally modifies task instance state
            *args: positional args to be passed into func
            **kwargs: kwargs to be to be passed into func
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.error_raised = False

    def __call__(self):
        try:
            self._func(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e
            self.error_raised = True
