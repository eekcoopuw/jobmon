from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING, Union

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.requester import http_request_ok, Requester


if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import (
        DistributorTaskInstance,
    )


logger = logging.getLogger(__name__)


class DistributorTask:
    def __init__(self, task_id: int, requester: Requester):
        self.task_id = task_id
        self.requested_resources = {}
        self.requester = requester

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.task_id != self.task_id:
            raise ValueError(
                f"task_id mismatch. TaskInstance={task_instance.task_id}. "
                f"Task={self.task_id}."
            )
        self.task_instance = task_instance
        task_instance.task = self

    def get_metadata(self):
        # app_route = f"/array/{array_id}"
        # return_code, response = self.requester.send_request(
        #     app_route=app_route, message={}, request_type="get", logger=logger
        # )
        # if http_request_ok(return_code) is False:
        #     raise InvalidResponse(
        #         f"Unexpected status code {return_code} from POST "
        #         f"request through route {app_route}. Expected "
        #         f"code 200. Response content: {response}"
        #     )
        self.name = ""

    def _load_requested_resources(self):
        self.requested_resources = {}
